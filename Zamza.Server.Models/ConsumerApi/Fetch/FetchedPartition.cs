using Zamza.Server.Models.Validators;

namespace Zamza.Server.Models.ConsumerApi.Fetch;

public sealed record FetchedPartition
{
    public string Topic { get; }
    public int Partition { get; }
    public long OwnershipEpoch { get; }
    public long KafkaOffset { get; }

    public FetchedPartition(
        string topic,
        int partition,
        long ownershipEpoch,
        long kafkaOffset)
    {
        ThrowBadRequest.IfEmpty(topic, "Fetched partition topic");
        
        Topic = topic;
        Partition = partition;
        OwnershipEpoch = ownershipEpoch;
        KafkaOffset = kafkaOffset;
    }
}