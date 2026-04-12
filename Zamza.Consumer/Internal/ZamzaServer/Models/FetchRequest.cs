namespace Zamza.Consumer.Internal.ZamzaServer.Models;

internal sealed record FetchRequest(
    string ConsumerId,
    string ConsumerGroup,
    int Limit,
    IReadOnlyCollection<FetchRequest.FetchedPartition> FetchedPartitions)
{
    internal sealed record FetchedPartition(
        string Topic,
        int Partition,
        long KafkaOffset,
        long OwnershipEpoch);
}