using System.ComponentModel.DataAnnotations;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

/// <summary>
/// Represents a single message stored in Zamza DLQ.
/// </summary>
public sealed class DLQMessageDto
{
    /// <summary>
    /// The consumer group the original message was consumed by.
    /// </summary>
    [Required]
    public required string ConsumerGroup { get; init; }
    
    /// <summary>
    /// The topic the original message was consumed from. 
    /// </summary>
    [Required]
    public required string Topic { get; init; }
    
    /// <summary>
    /// The partition the original message was consumed from. 
    /// </summary>
    [Required]
    public required int Partition { get; init; }
    
    /// <summary>
    /// The offset of the original message. 
    /// </summary>
    [Required]
    public required long Offset { get; init; }
    
    /// <summary>
    /// The headers of the original message. 
    /// </summary>
    /// <remarks>
    /// If no headers are set, an empty collection is returned.
    /// </remarks>
    [Required]
    public required Dictionary<string, byte[]> Headers { get; init; }
    
    /// <summary>
    /// The key of the original message. 
    /// </summary>
    public byte[]? Key { get; init; }
    
    /// <summary>
    /// The value of the original message. 
    /// </summary>
    public byte[]? Value { get; init; }
    
    /// <summary>
    /// The Kafka timestamp of the original message. 
    /// </summary>
    [Required]
    public required DateTimeOffset Timestamp { get; init; }
    
    /// <summary>
    /// The number of times the messages had been reprocessed
    /// before it was saved into the DLQ.
    /// </summary>
    [Required]
    public required int RetriesCount { get; init; }
    
    /// <summary>
    /// The UTC moment when the message was saved into the DLQ.
    /// </summary>
    [Required]
    public required DateTimeOffset SavedToDLQAtUTC { get; init; }
}