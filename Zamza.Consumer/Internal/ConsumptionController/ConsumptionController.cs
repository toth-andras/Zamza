using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.MessageProcessing;
using Zamza.Consumer.Internal.Models;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;
using Zamza.Consumer.Internal.ZamzaServer;
using Zamza.Consumer.Internal.ZamzaServer.Exceptions;
using Zamza.Consumer.Internal.ZamzaServer.Models;

namespace Zamza.Consumer.Internal.ConsumptionController;

internal sealed class ConsumptionController<TKey, TValue> : IConsumptionController<TKey, TValue>
{
    private readonly ConsumptionControllerState _state;
    private readonly PingRequest _pingRequest;

    private IReadOnlyList<PartitionOwnership> _currentlyOwnedPartitions;
    private readonly Dictionary<(string Topic, int Partition), PartitionOwnership> _knownConsumerGroupPartitionOwnerships;
    private readonly Dictionary<(string Topic, int Partition), long> _commitedKafkaOffsets;
    
    private readonly ZamzaConsumerSettings<TKey, TValue> _consumerConfig;
    private readonly IConsumer<TKey, TValue> _kafkaConsumer;
    private readonly IZamzaServerFacade<TKey, TValue> _zamzaServerFacade;
    private readonly IMessageProcessor<TKey, TValue> _messageProcessor;
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly ILogger<ConsumptionController<TKey, TValue>> _logger;

    public ConsumptionController(
        ZamzaConsumerSettings<TKey, TValue> consumerConfig,
        IConsumer<TKey, TValue> kafkaConsumer,
        IZamzaServerFacade<TKey, TValue> zamzaServerFacade,
        IMessageProcessor<TKey, TValue> messageProcessor,
        IDateTimeProvider dateTimeProvider,
        ILogger<ConsumptionController<TKey, TValue>> logger)
    {
        _consumerConfig = consumerConfig;
        _kafkaConsumer = kafkaConsumer;
        _zamzaServerFacade = zamzaServerFacade;
        _messageProcessor = messageProcessor;
        _dateTimeProvider = dateTimeProvider;
        _logger = logger;
        
        _state = new ConsumptionControllerState();
        _pingRequest = new PingRequest(consumerConfig.ConsumerId, consumerConfig.ConsumerGroup);
        _currentlyOwnedPartitions = [];
        _knownConsumerGroupPartitionOwnerships = [];
        _commitedKafkaOffsets = [];
    }

    public async Task RunMainLoop(CancellationToken token)
    {
        _logger.LogInformation(
            "Starting consumer with id {ConsumerId} for consumer group {ConsumerGroup}",
            _consumerConfig.ConsumerId,
            _consumerConfig.ConsumerGroup);

        var iteration = 0;

        while (!token.IsCancellationRequested)
        {
            if (_state.CurrentState is ConsumptionControllerStateEnum.Stopped)
            {
                return;
            }

            if (_state.CurrentState is ConsumptionControllerStateEnum.ZamzaServerNotAvailable)
            {
                await Ping(token).ConfigureAwait(false);
                continue;
            }
            
            if (_state.CurrentState is ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired)
            {
                await ClaimPartitionOwnership(ConsumptionControllerStateEnum.ProcessKafka, token)
                    .ConfigureAwait(false);
                iteration = 0;
                continue;
            }
            
            ++iteration;
            if (iteration == _consumerConfig.KafkaCallsPerZamzaCall + 1)
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
                await ProcessKafka(token).ConfigureAwait(false);
                continue;
            }

            if (_state.CurrentState is ConsumptionControllerStateEnum.ProcessZamza)
            {
                await ProcessZamza(token).ConfigureAwait(false);
                continue;
            }
        }
    }

    public void OnKafkaConsumerGroupRebalance()
    {
        _state.ChangeState(newState: ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired);
    }

    private async Task Ping(CancellationToken cancellationToken)
    {
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
        var partitionsToClaim = _kafkaConsumer.Assignment;
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
                        _consumerConfig.ConsumerId,
                        _consumerConfig.ConsumerGroup,
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
            _currentlyOwnedPartitions = claimsList;
            _state.ChangeState(desiredNextState);
        }
    }

    private async Task ProcessKafka(CancellationToken cancellationToken)
    {
        var offsets = _commitedKafkaOffsets
            .Select(offset => new TopicPartitionOffset(
                offset.Key.Topic,
                offset.Key.Partition,
                offset.Value))
            .ToList();
        foreach (var offset in offsets)
        {
            _kafkaConsumer.Seek(offset);   
        }
        
        var messages = Enumerable.Range(0, 5)
            .Select(_ => _kafkaConsumer.Consume(TimeSpan.FromMilliseconds(100)))
            .Where(res => res is not null)
            .Select(ToZamzaMessage)
            .ToList();
        
        // During the consume from Kafka, a rebalance handler may have been called.
        if (_state.CurrentState is ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired)
        {
            return;
        }

        if (messages.Count == 0)
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
            _consumerConfig.ConsumerId,
            _consumerConfig.ConsumerGroup,
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

        _kafkaConsumer.Commit(offsetsToCommit);

        foreach (var offset in offsetsToCommit)
        {
            _commitedKafkaOffsets[(offset.Topic, offset.Partition.Value)] = offset.Offset.Value;
        }
    }

    private async Task ProcessZamza(CancellationToken cancellationToken)
    {
        var fetchedPartitions = _currentlyOwnedPartitions
            .Select(partition => new FetchRequest.FetchedPartition(
                partition.Topic,
                partition.Partition,
                _commitedKafkaOffsets.GetValueOrDefault((partition.Topic, partition.Partition), 0L),
                partition.OwnerEpoch))
            .ToArray();
        
        FetchResult<TKey, TValue> fetchResult;
        try
        {
            fetchResult = await _zamzaServerFacade
                .Fetch(
                    new FetchRequest(
                        _consumerConfig.ConsumerId,
                        _consumerConfig.ConsumerGroup,
                        _consumerConfig.FetchLimit,
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
            _consumerConfig.ConsumerId,
            _consumerConfig.ConsumerGroup,
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

    private ZamzaMessage<TKey, TValue> ToZamzaMessage(ConsumeResult<TKey, TValue> consumeResult)
    {
        return new ZamzaMessage<TKey, TValue>(
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            consumeResult.Message.Headers.ToDictionary(
                header => header.Key,
                header => header.GetValueBytes()),
            consumeResult.Message.Key,
            consumeResult.Message.Value,
            consumeResult.Message.Timestamp.UtcDateTime,
            retriesCount: 0,
            maxRetriesCount: _consumerConfig.ProcessorConfig.MaxRetriesCount,
            processingDeadline: GetDeadline());

        DateTime? GetDeadline()
        {
            if (_consumerConfig.ProcessorConfig.ProcessingPeriod is null)
            {
                return null;
            }
            
            return _dateTimeProvider.UtcNow.Add(_consumerConfig.ProcessorConfig.ProcessingPeriod.Value);
        }
    }
}