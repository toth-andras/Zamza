using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.Configs;
using Zamza.Consumer.Internal.ConsumerState;
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
    private readonly ConsumerState.ConsumerState _state;
    private readonly PingRequest _pingRequest;
    private DateTime? _zamzaServerUnavailableSince; 

    private IReadOnlyList<PartitionOwnership> _currentlyOwnedPartitions;
    private readonly Dictionary<(string Topic, int Partition), PartitionOwnership> _knownConsumerGroupPartitionOwnerships;
    private readonly ZamzaConsumerConfig<TKey, TValue> _consumerConfig;
    
    private readonly IKafkaConsumerFacade<TKey, TValue> _kafkaConsumerFacade;
    private readonly IZamzaServerFacade<TKey, TValue> _zamzaServerFacade;
    private readonly IMessageProcessor<TKey, TValue> _messageProcessor;
    private readonly ILogger<ZamzaConsumer<TKey, TValue>> _logger;
    private readonly IDateTimeProvider _dateTimeProvider;

    public ZamzaConsumer(
        ZamzaConsumerConfig<TKey, TValue> consumerConfig,
        IKafkaConsumerFacade<TKey, TValue> kafkaConsumerFacade,
        IZamzaServerFacade<TKey, TValue> zamzaServerFacade,
        IMessageProcessor<TKey, TValue> messageProcessor,
        ILogger<ZamzaConsumer<TKey, TValue>> logger,
        IDateTimeProvider dateTimeProvider)
    {
        _consumerConfig = consumerConfig;
        
        _kafkaConsumerFacade = kafkaConsumerFacade;
        _kafkaConsumerFacade.OnConsumerGroupRebalance += OnConsumerGroupRebalance;
        _zamzaServerFacade = zamzaServerFacade;
        
        _messageProcessor = messageProcessor;
        _logger = logger;
        _dateTimeProvider = dateTimeProvider;
        
        _state = new ConsumerState.ConsumerState();
        _pingRequest = new PingRequest(
            _consumerConfig.MainInfo.ConsumerId,
            _consumerConfig.MainInfo.ConsumerGroup);
        _currentlyOwnedPartitions = [];
        _knownConsumerGroupPartitionOwnerships = [];
        _zamzaServerUnavailableSince = null;
    }
    
    public async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var scope = _logger.BeginScope(
            "ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            _consumerConfig.MainInfo.ConsumerGroup,
            _consumerConfig.MainInfo.ConsumerId);
        
        _logger.LogInformation("Consumer started");

        var iteration = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            if (_state.CurrentState is ConsumerStateEnum.Stopped)
            {
                _logger.LogInformation("Consumer stopped");
                return;
            }

            if (_state.CurrentState is ConsumerStateEnum.ZamzaServerNotAvailable)
            {
                await Ping(stoppingToken).ConfigureAwait(false);
                continue;
            }
            
            if (_state.CurrentState is ConsumerStateEnum.PartitionOwnershipClaimRequired)
            {
                await ClaimPartitionOwnership(ConsumerStateEnum.ProcessKafka, stoppingToken)
                    .ConfigureAwait(false);
                iteration = 0;
                continue;
            }
            
            ++iteration;
            if (iteration == _consumerConfig.ZamzaFetch.KafkaConsumesPerZamzaFetch + 1)
            {
                _state.ChangeState(ConsumerStateEnum.ProcessZamza);
                iteration = 0;
            }
            else
            {
                _state.ChangeState(ConsumerStateEnum.ProcessKafka);
            }

            if (_state.CurrentState is ConsumerStateEnum.ProcessKafka)
            {
                await ProcessKafka(stoppingToken).ConfigureAwait(false);
                continue;
            }

            if (_state.CurrentState is ConsumerStateEnum.ProcessZamza)
            {
                await ProcessZamza(stoppingToken).ConfigureAwait(false);
                continue;
            }
        }
    }

    public void Stop() {}

    private void OnConsumerGroupRebalance()
    {
        _logger.LogInformation("Kafka consumer group rebalance occured. Starting rebalance handling.");
        _state.ChangeState(newState: ConsumerStateEnum.PartitionOwnershipClaimRequired);
    }
    
    private async Task Ping(CancellationToken cancellationToken)
    {
        await Task.Delay(_consumerConfig.Ping.PingInterval, cancellationToken).ConfigureAwait(false);
        
        _logger.LogDebug("Ping request to Zamza.Server");
        var serverAvailable = await _zamzaServerFacade
            .Ping(
                _pingRequest, 
                cancellationToken)
            .ConfigureAwait(false);

        if (serverAvailable)
        {
            _zamzaServerUnavailableSince = null;
            _state.ChangeState(ConsumerStateEnum.PartitionOwnershipClaimRequired);
            _logger.LogInformation("Zamza.Server is available again. Resuming consumption");
            return;
        }

        if (_zamzaServerUnavailableSince is null)
        {
            _logger.LogTrace(
                "Started measuring Zamza.Server offline time. The max possible offline period is {MaxOfflineMs} ms",
                _consumerConfig.Ping.MaxOfflineTime.TotalMilliseconds);
            
            _zamzaServerUnavailableSince = _dateTimeProvider.UtcNow;
            return;
        }
        
        var serverOfflineTime = _dateTimeProvider.UtcNow - _zamzaServerUnavailableSince.Value;
        if (serverOfflineTime > _consumerConfig.Ping.MaxOfflineTime)
        {
            _logger.LogCritical("Zamza.Server is offline. Stopping the consumer");
            _state.ChangeState(ConsumerStateEnum.Stopped);
        }
    }

    private async Task ClaimPartitionOwnership(
        ConsumerStateEnum desiredNextState,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("ClaimPartitionOwnership request to Zamza.Server");
        
        const int initialOwnershipEpoch = 0;
        var partitionsToClaim = _kafkaConsumerFacade.AssignedPartitions;

        if (partitionsToClaim.Count == 0)
        {
            _logger.LogTrace("No partitions to claim. Ending claim");
            _state.ChangeState(desiredNextState);
            return;
        }
        
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

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            var claimedPartitionsToLog = claimsList
                .Select(claim => (claim.Topic, claim.Partition))
                .ToArray();
            
            _logger.LogDebug("Claimed partitions: {Partitions}", claimedPartitionsToLog);
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
            _logger.LogError("Zamza.Server is not available, switching to pinging");
            _state.ChangeState(newState: ConsumerStateEnum.ZamzaServerNotAvailable);
            return;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception, 
                "Unexpected exception occurred while ClaimPartitionOwnership request to Zamza.Server");
            return;
        }
        
        UpdateKnownPartitionOwnerships(result.ConsumerGroupPartitionOwnership);

        if (result.IsSuccessful)
        {
            _currentlyOwnedPartitions = partitionsToClaim
                .Select(partition => _knownConsumerGroupPartitionOwnerships[(partition.Topic, partition.Partition.Value)])
                .ToList();
            _state.ChangeState(desiredNextState);
            
            _logger.LogDebug("Successfully claimed partitions");
        }
    }

    private async Task ProcessKafka(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Consuming messages from Kafka");
        
        _kafkaConsumerFacade.Seek(_kafkaConsumerFacade.CommitedOffsets);
        
        var messages = _kafkaConsumerFacade.Consume();
        
        // During the consume from Kafka, a rebalance handler may have been called.
        if (_state.CurrentState is ConsumerStateEnum.PartitionOwnershipClaimRequired)
        {
            // Resetting consumer's offset so it can read the messages
            if (messages.Length > 0)
            {
                var offsetsToSeekTo = messages
                    .GroupBy(message => (message.Topic, message.Partition))
                    .Select(onePartitionMessages => onePartitionMessages.MinBy(message => message.Offset))
                    .Select(message => new TopicPartitionOffset(message.Topic, message.Partition, message.Offset))
                    .ToArray();
                _kafkaConsumerFacade.Seek(offsetsToSeekTo);
            }
            
            return;
        }

        if (messages.Length == 0)
        {
            _logger.LogDebug("No messages consumed from Kafka");
            return;
        }

        if (_logger.IsEnabled(LogLevel.Trace))
        {
            var consumedMessagesForLog = messages
                .Select(message => (message.Topic, message.Partition, message.Offset))
                .ToArray();
            _logger.LogDebug("Consumed messages (topic, partition, offset): {Messages}", consumedMessagesForLog);
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
            _logger.LogError("Zamza.Server is not available, switching to pinging");
            _state.ChangeState(ConsumerStateEnum.ZamzaServerNotAvailable);
            return;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception, 
                "Unexpected exception occurred while commiting message processing results in Zamza.Server");
            return;
        }
        
        UpdateKnownPartitionOwnerships(commitResult.ConsumerGroupPartitionOwnerships);

        if (commitResult.PartitionsWithIrrelevantOwnership.Count > 0)
        {
            _state.ChangeState(ConsumerStateEnum.PartitionOwnershipClaimRequired);
        }

        var irrelevantPartitions = commitResult.PartitionsWithIrrelevantOwnership
            .Select(partition => (Topic: partition.Topic, Partition: partition.Partition))
            .ToHashSet();
        
        var offsetsToCommit = processingResult
            .ProcessedMessages
            .Concat(processingResult.MessagesWithRetryableFailure.Select(message => message.Message))
            .Concat(processingResult.MessagesWithCompleteFailure.Select(message => message.Message))
            .Where(message => irrelevantPartitions.Contains((message.Topic, message.Partition)) is false)
            .GroupBy(message => (message.Topic, message.Partition))
            .Select(group => new TopicPartitionOffset(
                group.Key.Topic,
                group.Key.Partition,
                group.Max(message => message.Offset) + 1))
            .ToArray();

        var offsetsToSeekBack = processingResult
            .ProcessedMessages
            .Concat(processingResult.MessagesWithRetryableFailure.Select(message => message.Message))
            .Concat(processingResult.MessagesWithCompleteFailure.Select(message => message.Message))
            .Where(message => irrelevantPartitions.Contains((message.Topic, message.Partition)))
            .GroupBy(message => (message.Topic, message.Partition))
            .Select(group => new TopicPartitionOffset(
                group.Key.Topic, 
                group.Key.Partition, 
                group.Min(message => message.Offset)))
            .ToArray();
        
        _kafkaConsumerFacade.Commit(offsetsToCommit);
        _kafkaConsumerFacade.Commit(offsetsToSeekBack);

        if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogDebug(
                "Could not commit messages from partitions (topic, partition): {Partitions}",
                irrelevantPartitions);
        }
    }

    private async Task ProcessZamza(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Fetching messages from Zamza.Server");
        
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
            _state.ChangeState(newState: ConsumerStateEnum.ZamzaServerNotAvailable);
            _logger.LogError("Zamza.Server is not available, switching to ping");
            return;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Unexpected exception occurred while processing fetch from Zamza");
            return;
        }
        
        UpdateKnownPartitionOwnerships(fetchResult.ConsumerGroupPartitionOwnerships);

        if (fetchResult.IsFetchSuccessful is false)
        {
            _state.ChangeState(ConsumerStateEnum.PartitionOwnershipClaimRequired);
            return;
        }

        if (fetchResult.Messages.Count == 0)
        {
            _logger.LogTrace("No messages fetched from Zamza");
            return;
        }

        if (_logger.IsEnabled(LogLevel.Trace))
        {
            var messagesToLog = fetchResult.Messages
                .Select(message => (message.Topic, message.Partition, message.Offset))
                .ToArray();
            
            _logger.LogTrace("Fetched messages (topic, partition, offset): {Messages}", messagesToLog);
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
            _logger.LogError("Zamza.Server is not available, switching to pinging");
            _state.ChangeState(ConsumerStateEnum.ZamzaServerNotAvailable);
            return;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Unexpected exception occurred while committing message processing results in Zamza.Server");
            return;
        }
        
        UpdateKnownPartitionOwnerships(commitResult.ConsumerGroupPartitionOwnerships);

        if (commitResult.PartitionsWithIrrelevantOwnership.Count > 0)
        {
            _state.ChangeState(ConsumerStateEnum.PartitionOwnershipClaimRequired);
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