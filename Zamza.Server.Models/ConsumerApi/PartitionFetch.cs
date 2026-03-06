namespace Zamza.Server.Models.ConsumerApi;

public sealed record PartitionFetch(
    string Topic,
    int Partition,
    long KafkaOffset,
    long OwnershipEpoch);