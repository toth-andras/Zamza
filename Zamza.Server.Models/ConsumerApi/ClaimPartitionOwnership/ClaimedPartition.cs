namespace Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership;

public sealed record ClaimedPartition(
    string Topic,
    int Partition,
    long CurrentOwnerEpoch);