namespace Zamza.Consumer.Internal.Models;

internal sealed record PartitionOwnership(
    string Topic,
    int Partition,
    long OwnerEpoch);