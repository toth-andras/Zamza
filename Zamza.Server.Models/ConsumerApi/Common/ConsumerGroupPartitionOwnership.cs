using Zamza.Server.Models.Exceptions;
using Zamza.Server.Models.Validators;

namespace Zamza.Server.Models.ConsumerApi.Common;

public sealed record ConsumerGroupPartitionOwnership
{
    public const long InitialPartitionOwnerEpoch = 0;
    
    public string Topic { get;  }
    public int Partition { get; }
    public long OwnerEpoch { get; private set; }
    public string? OwnerConsumerId { get; private set; }
    public DateTimeOffset TimestampUtc { get; private set; }

    public ConsumerGroupPartitionOwnership(
        string topic,
        int partition,
        long ownerEpoch,
        string? ownerConsumerId,
        DateTimeOffset timestampUtc)
    {
        Throw.IfEmpty(topic, "Partition ownership topic name");
        Throw.IfNotUtc(timestampUtc, "Partition ownership timestamp");
        
        Topic = topic;
        Partition = partition;
        OwnerEpoch = ownerEpoch;
        OwnerConsumerId = ownerConsumerId;
        TimestampUtc = timestampUtc;
    }
    
    public static ConsumerGroupPartitionOwnership CreateForNotRegisteredPartition(
        string topic,
        int partition,
        string consumerId,
        DateTimeOffset timestampUtc)
    {
        return new ConsumerGroupPartitionOwnership(
            topic,
            partition,
            InitialPartitionOwnerEpoch,
            consumerId,
            timestampUtc);
    }

    public void SetNewOwner(
        string newOwnerConsumerId,
        long previousOwnerEpoch,
        DateTimeOffset timestampUtc)
    {
        Throw.IfEmpty(newOwnerConsumerId, "New partition owner ConsumerId");
        Throw.IfNotUtc(timestampUtc, "Partition ownership claim timestamp");
        if (previousOwnerEpoch != OwnerEpoch)
        {
            throw new BadRequestException("The epoch of previous partition owner is not correct");
        }

        OwnerEpoch++;
        OwnerConsumerId = newOwnerConsumerId;
        TimestampUtc = timestampUtc;
    }
};