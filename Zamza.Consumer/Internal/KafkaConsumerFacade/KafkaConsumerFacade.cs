using Confluent.Kafka;
using Zamza.Consumer.Internal.Configs;
using Zamza.Consumer.Internal.KafkaConsumerFacade.Mapping;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;

namespace Zamza.Consumer.Internal.KafkaConsumerFacade;

internal sealed class KafkaConsumerFacade<TKey, TValue> : IKafkaConsumerFacade<TKey, TValue>
{
    private readonly ZamzaConsumerConfig<TKey, TValue> _config;
    private readonly Dictionary<(string Topic, int Partition), long> _committedOffsets;
    private readonly IConsumer<TKey, TValue> _consumer;
    private readonly IDateTimeProvider _dateTimeProvider;

    public IReadOnlyCollection<TopicPartitionOffset> CommitedOffsets
    {
        get
        {
            return _committedOffsets
                .Select(offset =>
                    new TopicPartitionOffset(offset.Key.Topic, offset.Key.Partition, offset.Value))
                .ToArray();
        }
    }

    public IReadOnlyCollection<TopicPartition> AssignedPartitions => _consumer.Assignment;

    public KafkaConsumerFacade(
        ZamzaConsumerConfig<TKey, TValue> config,
        IDateTimeProvider dateTimeProvider)
    {
        _config = config;
        _committedOffsets = new Dictionary<(string Topic, int Partition), long>();
        _consumer = new ConsumerBuilder<TKey, TValue>(config.MainInfo.KafkaConsumerConfig)
            .SetPartitionsAssignedHandler((_, _) => { TriggerOnConsumerGroupRebalance(); })
            .SetPartitionsRevokedHandler((_, _) => { TriggerOnConsumerGroupRebalance(); })
            .SetPartitionsLostHandler((_, _) => { TriggerOnConsumerGroupRebalance(); })
            .Build();
        _consumer.Subscribe(config.MainInfo.Topics);
        _dateTimeProvider = dateTimeProvider;
    }
    
    public void Seek(IReadOnlyCollection<TopicPartitionOffset> offsets)
    {
        foreach (var offset in offsets)
        {
            _consumer.Seek(offset);
        }
    }
    
    public ZamzaMessage<TKey, TValue>[] Consume()
    {
        var consumeTimestamp = _dateTimeProvider.UtcNow;

        DateTime? processingDeadline = null;
        if (_config.MessageProcessor.ProcessingPeriod is not null)
        {
            processingDeadline = consumeTimestamp.Add(_config.MessageProcessor.ProcessingPeriod.Value);
        }

        var messages = Enumerable.Range(0, 5)
            .Select(_ => _consumer.Consume(TimeSpan.FromMilliseconds(100)))
            .Where(res => res is not null)
            .Select(res => res.ToZamzaMessage(
                _config.MessageProcessor.MaxRetriesCount,
                processingDeadline))
            .ToArray();
        
        return messages;
    }

    public void Commit(IReadOnlyCollection<TopicPartitionOffset> offsets)
    {
        _consumer.Commit(offsets);
        foreach (var offset in offsets)
        {
            _committedOffsets[(offset.Topic, offset.Partition.Value)] = offset.Offset.Value;
        }
    }


    public event Action? OnConsumerGroupRebalance;
    private void TriggerOnConsumerGroupRebalance()
    {
        Console.WriteLine("=================== Kafka rebalance ===================");
        OnConsumerGroupRebalance?.Invoke();
    }
}