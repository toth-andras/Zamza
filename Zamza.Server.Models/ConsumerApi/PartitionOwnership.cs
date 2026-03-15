namespace Zamza.Server.Models.ConsumerApi;

public sealed class PartitionOwnership
{
    public required string Topic { get; init; }
    
    public required int Partition { get; init; }
    
    public required long Epoch { get; set; }
    
    public string? ConsumerId { get; set; }
    
    public required DateTimeOffset Timestamp { get; set; }

    public void SetNewOwner(
        string consumerId,
        DateTimeOffset timestamp)
    {
        Epoch++;
        ConsumerId = consumerId;
        Timestamp = timestamp;
    }

    public static PartitionOwnership CreateNew(
        (string Topic, int PartitionValue) partition,
        string consumerId,
        DateTimeOffset timestamp)
    {
        const int initialEpoch = 1;
        return new PartitionOwnership
        {
            Topic = partition.Topic,
            Partition = partition.PartitionValue,
            Epoch = initialEpoch,
            ConsumerId = consumerId,
            Timestamp = timestamp
        };
    }
}