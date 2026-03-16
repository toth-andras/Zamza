namespace Zamza.Consumer.Models;

internal sealed record PartitionOwnership(
    string Topic,
    int Partition,
    long OwnershipEpoch);