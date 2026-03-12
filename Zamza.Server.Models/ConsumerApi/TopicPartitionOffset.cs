namespace Zamza.Server.Models.ConsumerApi;

public sealed record TopicPartitionOffset(
    string Topic,
    int Partition,
    long Offset);