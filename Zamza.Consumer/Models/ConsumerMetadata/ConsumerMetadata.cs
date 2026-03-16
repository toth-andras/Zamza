using Confluent.Kafka;

namespace Zamza.Consumer.Models.ConsumerMetadata;

internal sealed class ConsumerMetadata<TKey, TValue>
{
    private IReadOnlyCollection<TopicPartition>  _ownedPartitions;
    private IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership> _partitionOwnershipsOfConsumerGroup;
    
    public string ConsumerId { get; init; }
    public string ConsumerGroup { get; init; }
    public string BearerToken { get; private set; }
    
    public bool PartitionOwnershipUpdateRequired { get; private set; }

    public IReadOnlyCollection<TopicPartition> OwnedPartitions
    {
        get => _ownedPartitions;
        private set => _ownedPartitions = value;
    }

    public IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership> PartitionOwnershipsOfConsumerGroup
    {
        get => _partitionOwnershipsOfConsumerGroup;
        private set => _partitionOwnershipsOfConsumerGroup = value;
    }
    
    public IReadOnlyDictionary<(string Topic, int Partition), long> CommitedKafkaOffsets { get; private set; }
    public int MaxRetries { get; init; }
    public TimeSpan MinRetriesGap { get; init; }
    public TimeSpan? ProcessingPeriod { get; init; }
    
    public int ZamzaCallPerKafkaCalls { get; init; }

    public Func<ZamzaMessage<TKey, TValue>, TimeSpan>? RetryGapEvaluator { get; init; }
    
    public int FetchLimit { get; init; }

    public ConsumerMetadata(
        string consumerId,
        string consumerGroup,
        string bearerToken,
        IReadOnlyCollection<TopicPartition> ownedPartitions,
        IReadOnlyDictionary<(string Topic, int Partition),PartitionOwnership> partitionOwnershipsOfConsumerGroup,
        IReadOnlyDictionary<(string Topic, int Partition), long> commitedKafkaOffsets,
        int maxRetries,
        TimeSpan minRetriesGap,
        TimeSpan? processingPeriod,
        int zamzaCallPerKafkaCalls,
        Func<ZamzaMessage<TKey, TValue>, TimeSpan>? retryGapEvaluator,
        int fetchLimit)
    {
        ConsumerId = consumerId;
        ConsumerGroup = consumerGroup;
        BearerToken = bearerToken;
        _ownedPartitions = ownedPartitions;
        _partitionOwnershipsOfConsumerGroup = partitionOwnershipsOfConsumerGroup;
        CommitedKafkaOffsets = commitedKafkaOffsets;
        MaxRetries = maxRetries;
        MinRetriesGap = minRetriesGap;
        ProcessingPeriod = processingPeriod;
        ZamzaCallPerKafkaCalls = zamzaCallPerKafkaCalls;
        RetryGapEvaluator = retryGapEvaluator;
        PartitionOwnershipUpdateRequired = false;
        FetchLimit = fetchLimit;
    }

    public void MarkPartitionOwnershipUpdateAsRequired()
    {
        PartitionOwnershipUpdateRequired = true;
    }

    public void UpdatePartitionOwnership(
        IReadOnlyCollection<TopicPartition> ownedPartitions,
        IReadOnlyCollection<PartitionOwnership> partitionOwnershipsOfConsumerGroup)
    {
        _ownedPartitions = ownedPartitions;
        UpdateOwnershipEpochs(partitionOwnershipsOfConsumerGroup);
        PartitionOwnershipUpdateRequired = false;
    }

    public void UpdateOwnershipEpochs(IReadOnlyCollection<PartitionOwnership> ownershipEpochs)
    {
        _partitionOwnershipsOfConsumerGroup = ownershipEpochs.ToDictionary(
            ownership => (ownership.Topic, ownership.Partition),
            ownership => ownership);
    }

    public void UpdateKafkaOffset(IReadOnlyCollection<TopicPartitionOffset> offsets)
    {
        CommitedKafkaOffsets = offsets.ToDictionary(
            offset => (offset.Topic, offset.Partition.Value),
            offset => offset.Offset.Value);
    }
}