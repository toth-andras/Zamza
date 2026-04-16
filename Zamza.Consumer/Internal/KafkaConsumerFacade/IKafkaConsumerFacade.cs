using Confluent.Kafka;

namespace Zamza.Consumer.Internal.KafkaConsumerFacade;

internal interface IKafkaConsumerFacade<TKey, TValue>
{
    public event Action OnConsumerGroupRebalance;
    public IReadOnlyCollection<TopicPartitionOffset> CommitedOffsets { get; }
    public IReadOnlyCollection<TopicPartition> AssignedPartitions { get; }

    public void Seek(IReadOnlyCollection<TopicPartitionOffset> offsets);
    public ZamzaMessage<TKey, TValue>[] Consume();
    public void Commit(IReadOnlyCollection<TopicPartitionOffset> offsets);
}