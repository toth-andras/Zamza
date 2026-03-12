namespace Zamza.Server.Models.ConsumerApi;

public sealed record ConsumerPartitionOwnershipsSet
{
    public string ConsumerGroup { get; init; }
    public IReadOnlyDictionary<(string Topic, int Partition), long> ConsumerPartitionOwnerships { get; }
    
    public int Count => ConsumerPartitionOwnerships.Count;

    public ConsumerPartitionOwnershipsSet(
        string consumerGroup,
        IReadOnlyDictionary<(string Topic, int Partition), long> consumerPartitionOwnerships)
    {
        ArgumentNullException.ThrowIfNull(consumerGroup, nameof(consumerGroup));
        ArgumentNullException.ThrowIfNull(consumerPartitionOwnerships, nameof(consumerPartitionOwnerships));
        
        ConsumerGroup = consumerGroup;
        ConsumerPartitionOwnerships = consumerPartitionOwnerships;
    }

    public bool TryGetOwnershipEpochForPartition(string topic, int partition, out long ownershipEpoch)
    {
        return ConsumerPartitionOwnerships.TryGetValue((topic, partition), out ownershipEpoch);
    }

    public (string[] Topics, int[] Partitions) GetTopicsAndPartitions()
    {
        var topics = new string[Count];
        var partitions = new int[Count];

        var index = 0;
        foreach (var topicPartition in ConsumerPartitionOwnerships.Keys)
        {
            topics[index] = topicPartition.Topic;
            partitions[index] = topicPartition.Partition;
            index++;
        }
        
        return (topics, partitions);
    }
}