using System.Collections;

namespace Zamza.Server.Models.ConsumerApi.Common;

public sealed record ConsumerGroupPartitionOwnershipSet 
    : IEnumerable<ConsumerGroupPartitionOwnership>
{
    private readonly Dictionary<(string Topic, int Partition), ConsumerGroupPartitionOwnership> _partitionOwnerships;
    
    public string ConsumerGroup { get; }

    public int PartitionCount => _partitionOwnerships.Count;

    public ConsumerGroupPartitionOwnershipSet(
        string consumerGroup,
        IEnumerable<ConsumerGroupPartitionOwnership> partitionOwnerships)
    {
        ConsumerGroup = consumerGroup;
        _partitionOwnerships = partitionOwnerships.ToDictionary(
            partitionOwnership => (partitionOwnership.Topic, partitionOwnership.Partition),
            partitionOwnership => partitionOwnership);
    }

    public long GetOwnerEpochForPartition(string topic, int partition)
    {
        return _partitionOwnerships.TryGetValue((topic, partition), out var ownership)
            ? ownership.OwnerEpoch
            : ConsumerGroupPartitionOwnership.InitialPartitionOwnerEpoch;
    }

    public bool IsPartitionRegistered(string topic, int partition)
    {
        return _partitionOwnerships.ContainsKey((topic, partition));
    }

    public void SetNewPartitionOwner(
        string topic,
        int partition,
        long previousOwnerEpoch,
        string newOwnerConsumerId,
        DateTimeOffset timestamp)
    {
        var ownership = _partitionOwnerships.TryGetValue((topic, partition), out var existingOwnership)
            ? existingOwnership
            : ConsumerGroupPartitionOwnership.CreateForNotRegisteredPartition(
                ConsumerGroup,
                topic, 
                partition,
                newOwnerConsumerId, 
                timestamp);

        ownership.SetNewOwner(
            newOwnerConsumerId,
            previousOwnerEpoch,
            timestamp);
        
        _partitionOwnerships[(topic, partition)] = ownership;
    }
    
    public IEnumerator<ConsumerGroupPartitionOwnership> GetEnumerator()
    {
        return _partitionOwnerships.Values.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}