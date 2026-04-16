using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.Configs;
using Zamza.Consumer.Internal.ConsumptionController;
using Zamza.Consumer.Internal.KafkaConsumerFacade;
using Zamza.Consumer.Internal.MessageProcessing;
using Zamza.Consumer.Internal.Models;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;
using Zamza.Consumer.Internal.ZamzaServer;
using Zamza.Consumer.Internal.ZamzaServer.Exceptions;
using Zamza.Consumer.Internal.ZamzaServer.Models;

namespace Zamza.Consumer.Internal;

internal sealed class ZamzaConsumer<TKey, TValue> : IZamzaConsumer
{
    private readonly ConsumptionControllerState _state;
    private readonly PingRequest _pingRequest;

    private IReadOnlyList<PartitionOwnership> _currentlyOwnedPartitions;
    private readonly Dictionary<(string Topic, int Partition), PartitionOwnership> _knownConsumerGroupPartitionOwnerships;
    private readonly ZamzaConsumerConfig<TKey, TValue> _consumerConfig;
    
    private readonly IKafkaConsumerFacade<TKey, TValue> _kafkaConsumerFacade;
    private readonly IZamzaServerFacade<TKey, TValue> _zamzaServerFacade;
    private readonly IMessageProcessor<TKey, TValue> _messageProcessor;
    private readonly ILogger<ZamzaConsumer<TKey, TValue>> _logger;

    public ZamzaConsumer(
        ZamzaConsumerConfig<TKey, TValue> consumerConfig,
        IKafkaConsumerFacade<TKey, TValue> kafkaConsumerFacade,
        IZamzaServerFacade<TKey, TValue> zamzaServerFacade,
        IMessageProcessor<TKey, TValue> messageProcessor,
        ILogger<ZamzaConsumer<TKey, TValue>> logger)
    {
        _consumerConfig = consumerConfig;
        
        _kafkaConsumerFacade = kafkaConsumerFacade;
        _kafkaConsumerFacade.OnConsumerGroupRebalance += OnConsumerGroupRebalance;
        _zamzaServerFacade = zamzaServerFacade;
        
        _messageProcessor = messageProcessor;
        _logger = logger;
        
        _state = new ConsumptionControllerState();
        _pingRequest = new PingRequest(
            _consumerConfig.MainInfo.ConsumerId,
            _consumerConfig.MainInfo.ConsumerGroup);
        _currentlyOwnedPartitions = [];
        _knownConsumerGroupPartitionOwnerships = [];
    }
    
    public async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting consumer with id {ConsumerId} for consumer group {ConsumerGroup}",
            _consumerConfig.MainInfo.ConsumerId,
            _consumerConfig.MainInfo.ConsumerGroup);

        var iteration = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            if (_state.CurrentState is ConsumptionControllerStateEnum.Stopped)
            {
                return;
            }

            if (_state.CurrentState is ConsumptionControllerStateEnum.ZamzaServerNotAvailable)
            {
                await Ping(stoppingToken).ConfigureAwait(false);
                continue;
            }
            
            if (_state.CurrentState is ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired)
            {
                await ClaimPartitionOwnership(ConsumptionControllerStateEnum.ProcessKafka, stoppingToken)
                    .ConfigureAwait(false);
                iteration = 0;
                continue;
            }
            
            ++iteration;
            if (iteration == _consumerConfig.ZamzaFetch.KafkaConsumesPerZamzaFetch + 1)
            {
                _state.ChangeState(ConsumptionControllerStateEnum.ProcessZamza);
                iteration = 0;
            }
            else
            {
                _state.ChangeState(ConsumptionControllerStateEnum.ProcessKafka);
            }

            if (_state.CurrentState is ConsumptionControllerStateEnum.ProcessKafka)
            {
                await ProcessKafka(stoppingToken).ConfigureAwait(false);
                continue;
            }

            if (_state.CurrentState is ConsumptionControllerStateEnum.ProcessZamza)
            {
                await ProcessZamza(stoppingToken).ConfigureAwait(false);
                continue;
            }
        }
    }

    public void Stop() {}

    private void OnConsumerGroupRebalance()
    {
        _state.ChangeState(newState: ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired);
    }
    
    private async Task Ping(CancellationToken cancellationToken)
    {
        await Task.Delay(_consumerConfig.Ping.PingInterval, cancellationToken).ConfigureAwait(false);
        var serverAvailable = await _zamzaServerFacade
            .Ping(
                _pingRequest, 
                cancellationToken)
            .ConfigureAwait(false);

        if (serverAvailable)
        {
            _state.ChangeState(ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired);
            _logger.LogInformation("Zamza server is available again. Resuming consumption");
            return;
        }
        
        // TODO: add server offline for too long flow
    }

    private async Task ClaimPartitionOwnership(
        ConsumptionControllerStateEnum desiredNextState,
        CancellationToken cancellationToken)
    {
        var partitionsToClaim = _kafkaConsumerFacade.AssignedPartitions;
        const int initialOwnershipEpoch = 0;
        
        var claimsList = new List<PartitionOwnership>(partitionsToClaim.Count);

        foreach (var partitionToClaim in partitionsToClaim)
        {
            var isPartitionRegistered = _knownConsumerGroupPartitionOwnerships.ContainsKey(
                (partitionToClaim.Topic, partitionToClaim.Partition.Value));
            
            if (isPartitionRegistered is false)
            {
                _knownConsumerGroupPartitionOwnerships[(partitionToClaim.Topic, partitionToClaim.Partition.Value)] = 
                    new PartitionOwnership(partitionToClaim.Topic, partitionToClaim.Partition.Value, initialOwnershipEpoch);
            }
            
            claimsList.Add(_knownConsumerGroupPartitionOwnerships[(partitionToClaim.Topic, partitionToClaim.Partition.Value)]);
        }

        ClaimPartitionOwnershipResult result;
        try
        {
            result = await _zamzaServerFacade
                .ClaimPartitionOwnership(
                    new ClaimPartitionOwnershipRequest(
                        _consumerConfig.MainInfo.ConsumerId,
                        _consumerConfig.MainInfo.ConsumerGroup,
                        claimsList),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ZamzaException zamzaException) when (zamzaException.Code is ZamzaErrorCode.ServerUnavailable)
        {
            _logger.LogError("Zamza server is not available, switching to ping");
            _state.ChangeState(newState: ConsumptionControllerStateEnum.ZamzaServerNotAvailable);
            return;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Unexpected exception occurred while claiming partition ownership");
            return;
        }
        
        UpdateKnownPartitionOwnerships(result.ConsumerGroupPartitionOwnership);

        if (result.IsSuccessful)
        {
            _currentlyOwnedPartitions = partitionsToClaim
                .Select(partition => _knownConsumerGroupPartitionOwnerships[(partition.Topic, partition.Partition.Value)])
                .ToList();
            _state.ChangeState(desiredNextState);
        }
    }

    private async Task ProcessKafka(CancellationToken cancellationToken)
    {
        _kafkaConsumerFacade.Seek(_kafkaConsumerFacade.CommitedOffsets);
        
        var messages = _kafkaConsumerFacade.Consume();
        
        // During the consume from Kafka, a rebalance handler may have been called.
        if (_state.CurrentState is ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired)
        {
            return;
        }

        if (messages.Length == 0)
        {
            return;
        }
        
        var processingResult = await _messageProcessor
            .ProcessMessages(
                _consumerConfig,
                messages,
                cancellationToken)
            .ConfigureAwait(false);
        
        var commitRequest = new CommitRequest<TKey, TValue>(
            _consumerConfig.MainInfo.ConsumerId,
            _consumerConfig.MainInfo.ConsumerGroup,
            _currentlyOwnedPartitions,
            processingResult.ProcessedMessages,
            processingResult.MessagesWithRetryableFailure,
            processingResult.MessagesWithCompleteFailure);
        
        CommitResult commitResult;
        try
        {
            commitResult = await _zamzaServerFacade
                .Commit(commitRequest, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ZamzaException exception) when (exception.Code is ZamzaErrorCode.ServerUnavailable)
        {
            _logger.LogError("Zamza server is not available, switching to ping");
            _state.ChangeState(ConsumptionControllerStateEnum.ZamzaServerNotAvailable);
            return;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Unexpected exception occurred while commit to Zamza");
            return;
        }
        
        UpdateKnownPartitionOwnerships(commitResult.ConsumerGroupPartitionOwnerships);

        if (commitResult.PartitionsWithIrrelevantOwnership.Count > 0)
        {
            _state.ChangeState(ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired);
        }

        var faultyPartitions = commitResult.PartitionsWithIrrelevantOwnership
            .Select(partition => (Topic: partition.Topic, Partition: partition.Partition))
            .ToHashSet();
        
        var offsetsToCommit = processingResult
            .ProcessedMessages
            .Concat(processingResult.MessagesWithRetryableFailure.Select(message => message.Message))
            .Concat(processingResult.MessagesWithCompleteFailure.Select(message => message.Message))
            .Where(message => faultyPartitions.Contains((message.Topic, message.Partition)) is false)
            .GroupBy(message => (message.Topic, message.Partition))
            .Select(group => new TopicPartitionOffset(
                group.Key.Topic,
                group.Key.Partition,
                group.Max(message => message.Offset) + 1))
            .ToArray();
        
        _kafkaConsumerFacade.Commit(offsetsToCommit);
    }

    private async Task ProcessZamza(CancellationToken cancellationToken)
    {
        var commitedOffsets = _kafkaConsumerFacade.CommitedOffsets.ToDictionary(
            tpo => (tpo.Topic, tpo.Partition.Value),
            tpo => tpo.Offset.Value);
        
        var fetchedPartitions = _currentlyOwnedPartitions
            .Select(partition => new FetchRequest.FetchedPartition(
                partition.Topic,
                partition.Partition,
                commitedOffsets.GetValueOrDefault((partition.Topic, partition.Partition), 0L),
                partition.OwnerEpoch))
            .ToArray();
        
        FetchResult<TKey, TValue> fetchResult;
        try
        {
            fetchResult = await _zamzaServerFacade
                .Fetch(
                    new FetchRequest(
                        _consumerConfig.MainInfo.ConsumerId,
                        _consumerConfig.MainInfo.ConsumerGroup,
                        _consumerConfig.ZamzaFetch.FetchLimit,
                        fetchedPartitions),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ZamzaException zamzaException) when (zamzaException.Code is ZamzaErrorCode.ServerUnavailable)
        {
            _state.ChangeState(newState: ConsumptionControllerStateEnum.ZamzaServerNotAvailable);
            _logger.LogError("Zamza server is not available, switching to ping");
            return;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Unexpected exception occurred while processing fetch from Zamza");
            return;
        }
        
        UpdateKnownPartitionOwnerships(fetchResult.ConsumerGroupPartitionOwnerships);

        if (fetchResult.IsFetchSuccessful is false)
        {
            _state.ChangeState(ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired);
            return;
        }

        if (fetchResult.Messages.Count == 0)
        {
            return;
        }

        var processingResult = await _messageProcessor
            .ProcessMessages(
                _consumerConfig,
                fetchResult.Messages,
                cancellationToken)
            .ConfigureAwait(false);

        var commitRequest = new CommitRequest<TKey, TValue>(
            _consumerConfig.MainInfo.ConsumerId,
            _consumerConfig.MainInfo.ConsumerGroup,
            _currentlyOwnedPartitions,
            processingResult.ProcessedMessages,
            processingResult.MessagesWithRetryableFailure,
            processingResult.MessagesWithCompleteFailure);
        
        CommitResult commitResult;
        try
        {
            commitResult = await _zamzaServerFacade
                .Commit(commitRequest, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ZamzaException zamzaException) when (zamzaException.Code is ZamzaErrorCode.ServerUnavailable)
        {
            _logger.LogError("Zamza server is not available, switching to ping");
            _state.ChangeState(ConsumptionControllerStateEnum.ZamzaServerNotAvailable);
            return;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Unexpected exception occurred while processing commit in Zamza");
            return;
        }
        
        UpdateKnownPartitionOwnerships(commitResult.ConsumerGroupPartitionOwnerships);

        if (commitResult.PartitionsWithIrrelevantOwnership.Count > 0)
        {
            _state.ChangeState(ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired);
        }
    }

    private void UpdateKnownPartitionOwnerships(
        IReadOnlyCollection<PartitionOwnership> newConsumerGroupPartitionOwnerships)
    {
        foreach (var newPartitionOwnership in newConsumerGroupPartitionOwnerships)
        {
            _knownConsumerGroupPartitionOwnerships[(newPartitionOwnership.Topic, newPartitionOwnership.Partition)] = newPartitionOwnership;
        }
    }
}