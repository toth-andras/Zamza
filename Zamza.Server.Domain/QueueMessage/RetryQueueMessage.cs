namespace Zamza.Server.Domain.QueueMessage;

/// <summary>
/// This model represents a Kafka message, stored inside
/// Zamza for repeated processing. 
/// </summary>
public sealed class RetryQueueMessage
{
    /// <summary>
    /// Kafka consumer group name.
    /// </summary>
    public required string ConsumerGroup { get; init; }
    
    /// <summary>
    /// Kafka topic name.
    /// </summary>
    public required string Topic { get; init; }
    
    /// <summary>
    /// Partition of the topic.
    /// </summary>
    public required int Partition { get; init; }
    
    /// <summary>
    /// Offset of the message.
    /// </summary>
    public required long Offset { get; init; }
    
    /// <summary>
    /// Headers of the message.
    /// </summary>
    public byte[]? Headers { get; init; }
    
    /// <summary>
    /// Key of the message.
    /// </summary>
    public byte[]? Key { get; init; }
    
    /// <summary>
    /// Value of the message.
    /// </summary>
    public byte[]? Value { get; init; }
    
    public required DateTimeOffset Timestamp { get; init; }
    
    /// <summary>
    /// Represent the number bi
    /// </summary>
    public required int MaxRetries { get; init; }
    
    /// <summary>
    /// Time gap between attempts
    /// to reprocess the message.
    /// </summary>
    public required long MinRetriesGapMs { get; init; }
    
    /// <summary>
    /// A point in time after which the message will not be sent to
    /// repeated processing, event though the number of allowed attempts
    /// is not exhausted.
    /// </summary>
    public DateTimeOffset? ProcessingDeadline { get; init; }
    
    /// <summary>
    /// The next retry attempt will be applied not earlier than this time.
    /// </summary>
    public required DateTimeOffset NextRetryAfter { get; init; }
    
    /// <summary>
    /// Timestamp of the last retry attempt.
    /// </summary>
    public DateTimeOffset? LastRetryAt { get; init; }
    
    /// <summary>
    /// The number of reprocessing attempts already applied.
    /// </summary>
    public required int RetriesCount { get; init; }
    
    /// <summary>
    /// Custom description of the reason the message was sent
    /// to retry queue (is provided by user).
    /// </summary>
    public string? RetryReason { get; init; }
}