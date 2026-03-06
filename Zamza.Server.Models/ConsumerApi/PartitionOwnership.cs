namespace Zamza.Server.Models.ConsumerApi;

public sealed class PartitionOwnership
{
    public required string Topic { get; set; }
    
    public required int Partition { get; set; }
    
    public required long Epoch { get; set; }
    
    public string? ConsumerId { get; set; }
    
    public DateTimeOffset Timestamp { get; set; }
}