using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership;

public sealed record PartitionOwnershipClaimSet
{
    public string ConsumerId { get; }
    public string ConsumerGroup { get; }
    public IReadOnlyList<ClaimedPartition> Partitions { get; }
    public DateTimeOffset TimestampUtc { get; }

    public PartitionOwnershipClaimSet(
        string consumerId,
        string consumerGroup,
        IEnumerable<ClaimedPartition> partitions,
        DateTimeOffset timestamp)
    {
        ThrowIfEmpty(consumerId);
        ThrowIfEmpty(consumerGroup);
        ThrowIfNotUtc(timestamp);
        
        ConsumerId = consumerId;
        ConsumerGroup = consumerGroup;
        Partitions = partitions
            .DistinctBy(partition => (partition.Topic, partition.Partition))
            .ToList()
            .AsReadOnly();
        
        TimestampUtc = timestamp;
    }

    private static void ThrowIfEmpty(string str)
    {
        if (string.IsNullOrWhiteSpace(str))
        {
            throw new BadRequestException("ConsumerId and consumer group in partition claims cannot be empty");
        }
    }

    private static void ThrowIfNotUtc(DateTimeOffset timestamp)
    {
        if (timestamp.Offset != TimeSpan.Zero)
        {
            throw new BadRequestException("The timestamp for partitions claim must be provided in UTC");
        } 
    }
}