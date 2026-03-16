using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.CustomProcessing;
using Zamza.Consumer.Factories;
using Zamza.Consumer.Models;
using Zamza.Consumer.Models.ConsumerMetadata;
using Zamza.Consumer.ServerFacade;

namespace Zamza.Consumer;

internal class ConsumptionController<TKey, TValue> : BackgroundService
{
    private readonly IConsumer<TKey, TValue> _kafkaConsumer;
    private readonly ZamzaServerFacade<TKey, TValue> _zamzaServerFacade;
    private readonly ConsumerMetadata<TKey, TValue> _metadata;
    private readonly ICustomProcessor<TKey, TValue> _customProcessor;
    private readonly ILogger<ConsumptionController<TKey, TValue>> _logger;
    
    internal ConsumptionController(
        IConsumer<TKey, TValue> kafkaConsumer,
        ZamzaServerFacade<TKey, TValue> zamzaServerFacade,
        ConsumerMetadata<TKey, TValue> metadata,
        ICustomProcessor<TKey, TValue> customProcessor,
        ILogger<ConsumptionController<TKey, TValue>> logger)
    {
        _kafkaConsumer = kafkaConsumer;
        _zamzaServerFacade = zamzaServerFacade;
        _metadata = metadata;
        _customProcessor = customProcessor;
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await MainLoop(stoppingToken);
        }
        catch (Exception ex)
        {
            // pass
        }
    }

    private async Task MainLoop(CancellationToken cancellationToken)
    {
        long iteration = 1;
        while (cancellationToken.IsCancellationRequested is false)
        {
            if (iteration % (_metadata.ZamzaCallPerKafkaCalls + 1) == 0)
            {
                await ZamzaFlow(cancellationToken);
            }
            else
            {
                await KafkaFlow(cancellationToken);
            }
            
            iteration++;
        }
    }

    private async Task UpdatePartitionOwnerships(CancellationToken cancellationToken)
    {
        var newPartitions = _kafkaConsumer.Assignment.Select(p => p).ToList();
        
        var response = await _zamzaServerFacade.ClaimOwnership(
            newPartitions,
            _metadata,
            cancellationToken).ConfigureAwait(false);
        
        
        _metadata.UpdatePartitionOwnership(newPartitions, response.ConsumerGroupOwnerships);
    }

    private async Task KafkaFlow(CancellationToken cancellationToken)
    {
        var messages = Enumerable.Range(0, 5)
            .Select(_ => _kafkaConsumer.Consume(TimeSpan.FromMilliseconds(100)))
            .Where(res => res is not null)
            .Select(res => ZamzaMessageFactoryForKafka.Create(res, _metadata))
            .ToList();

        if (messages.Count == 0)
        {
            return;
        }

        var processed = new List<ZamzaMessage<TKey, TValue>>();
        var failed = new List<(ZamzaMessage<TKey, TValue>, TimeSpan)>();
        var poisoned = new List<ZamzaMessage<TKey, TValue>>();

        foreach (var message in messages)
        {
            ProcessVerdict processVerdict;
            try
            {
                processVerdict = await _customProcessor.Process(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                processVerdict = ProcessVerdict.Poisoned;
            }

            if (processVerdict is ProcessVerdict.Failed && message.RetriesCount >= message.MaxRetries)
            {
                processVerdict = ProcessVerdict.Poisoned;
            }

            switch (processVerdict)
            {
                case ProcessVerdict.Processed:
                    processed.Add(message);
                    break;
                case ProcessVerdict.Failed:
                    failed.Add((message, GetProcessingGap(message)));
                    break;
                case ProcessVerdict.Poisoned:
                    poisoned.Add(message);
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        if (_metadata.PartitionOwnershipUpdateRequired)
        {
            await UpdatePartitionOwnerships(cancellationToken).ConfigureAwait(false);
        }

        var zamzaCommitResult = await _zamzaServerFacade.Commit(
            _metadata,
            processed,
            failed,
            poisoned,
            cancellationToken).ConfigureAwait(false);
        
        _metadata.UpdateOwnershipEpochs(zamzaCommitResult.PartitionOwnershipsForConsumerGroup);

        var faultyPartitions = zamzaCommitResult.ProhibitedTopicsMessages
            .Concat(zamzaCommitResult.UnownedPartitionsMessages)
            .Select(res => (res.Topic, res.Partition))
            .ToHashSet();

        var offsetsToCommit = messages
            .Where(m => faultyPartitions.Contains((m.Topic, m.Partition)) is false)
            .GroupBy(m => (m.Topic, m.Partition))
            .Select(g => new TopicPartitionOffset(
                g.Key.Topic, g.Key.Partition, g.Max(m => m.Offset) + 1))
            .ToList();
        
        _metadata.UpdateKafkaOffset(offsetsToCommit);
        
        _kafkaConsumer.Commit(offsetsToCommit);
    }

    private async Task ZamzaFlow(CancellationToken cancellationToken)
    {
        if (_metadata.PartitionOwnershipUpdateRequired)
        {
            await UpdatePartitionOwnerships(cancellationToken).ConfigureAwait(false);
        }

        var fetchResponse = await _zamzaServerFacade
            .Fetch(_metadata, cancellationToken)
            .ConfigureAwait(false);
        
        _metadata.UpdateOwnershipEpochs(fetchResponse.PartitionOwnershipsForConsumerGroup);

        if (fetchResponse.Messages.Count == 0)
        {
            return;
        }
        
        var processed = new List<ZamzaMessage<TKey, TValue>>();
        var failed = new List<(ZamzaMessage<TKey, TValue>, TimeSpan)>();
        var poisoned = new List<ZamzaMessage<TKey, TValue>>();

        foreach (var message in fetchResponse.Messages)
        {
            ProcessVerdict processVerdict;
            message.IncreaseRetriesCount();
            try
            {
                processVerdict = await _customProcessor.Process(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                processVerdict = ProcessVerdict.Poisoned;
            }

            if (processVerdict is ProcessVerdict.Failed && message.RetriesCount >= message.MaxRetries)
            {
                processVerdict = ProcessVerdict.Poisoned;
            }

            switch (processVerdict)
            {
                case ProcessVerdict.Processed:
                    processed.Add(message);
                    break;
                case ProcessVerdict.Failed:
                    failed.Add((message, GetProcessingGap(message)));
                    break;
                case ProcessVerdict.Poisoned:
                    poisoned.Add(message);
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        var commitResult = await _zamzaServerFacade.Commit(
            _metadata,
            processed,
            failed,
            poisoned,
            cancellationToken).ConfigureAwait(false);
        
        _metadata.UpdateOwnershipEpochs(commitResult.PartitionOwnershipsForConsumerGroup);
    }
    
    private TimeSpan GetProcessingGap(ZamzaMessage<TKey, TValue> message)
    {
        if (_metadata.RetryGapEvaluator is null)
        {
            return _metadata.MinRetriesGap;
        }

        var gap = _metadata.RetryGapEvaluator(message);
        
        return gap > _metadata.MinRetriesGap
            ? gap
            : _metadata.MinRetriesGap;
    }
}