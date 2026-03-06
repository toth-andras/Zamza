namespace Zamza.Server.Models.Messages;

public sealed class ConsumerApiMessage
{
    public required string ConsumerGroup { get; set; }
    
    public required string Topic { get; set; }
    
    public required int Partition { get; set; }
    
    public required long Offset { get; set; }
    
    public required Dictionary<string, byte[]> Headers { get; set; }
    
    public byte[]? Key { get; set; }
    
    public byte[]? Value { get; set; }
    
    public required DateTimeOffset Timestamp { get; set; }
    
    public required int MaxRetries { get; set; }
    
    public required long MinRetriesGapMs { get; set; }
    
    public long? ProcessingPeriodMs { get; set; }
    
    public required int RetriesCount { get; set; }
}