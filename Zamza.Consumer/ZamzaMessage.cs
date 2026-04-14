namespace Zamza.Consumer;

/// <summary>
/// Represents a message consumed from a Kafka cluster or a Zamza.Server.
/// </summary>
public sealed class ZamzaMessage<TKey, TValue>
{
    /// <summary>
    /// The topic associated with the message.
    /// </summary>
    public string Topic { get; private init; }
    
    /// <summary>
    /// The partition associated with the message.
    /// </summary>
    public int Partition { get; private init; }
    
    /// <summary>
    /// The partition offset associated with the message.
    /// </summary>
    public long Offset { get; private init; }
    
    /// <summary>
    /// The collection of message headers. Specifying null or an empty collection are equivalent.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]>? Headers { get; private init; }
    
    /// <summary>
    /// The message key value.
    /// </summary>
    public TKey? Key { get; private init; }
    
    /// <summary>
    /// The message value.
    /// </summary>
    public TValue? Value { get; private init; }
    
    /// <summary>
    /// The message timestamp. 
    /// </summary>
    public DateTime Timestamp { get; private init; }
    
    /// <summary>
    /// The number of reprocessing attempts the message has gone through.
    /// Zero if the message was consumed from Kafka.
    /// </summary>
    public int RetriesCount { get; private set; }
    
    /// <summary>
    /// No more than this many retry attempts will be executed.
    /// </summary>
    /// <remarks>
    /// If the maximum number of retry attempts is reached, the message
    /// is considered to be poisoned.
    /// </remarks>
    public int MaxRetriesCount { get; private init; }
    
    /// <summary>
    /// If set, represents the point in time after which the message,
    /// if not processed correctly, must be considered to be poisoned.
    /// </summary>
    public DateTime? ProcessingDeadline { get; private init; }

    public ZamzaMessage(
        string topic,
        int partition,
        long offset,
        IReadOnlyDictionary<string, byte[]>? headers,
        TKey? key,
        TValue? value,
        DateTime timestamp,
        int retriesCount,
        int maxRetriesCount,
        DateTime? processingDeadline)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Headers = headers;
        Key = key;
        Value = value;
        Timestamp = timestamp;
        RetriesCount = retriesCount;
        MaxRetriesCount = maxRetriesCount;
        ProcessingDeadline = processingDeadline;
    }

    internal void IncrementRetriesCount()
    {
        RetriesCount++;
    }
}