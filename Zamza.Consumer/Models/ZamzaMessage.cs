using Confluent.Kafka;

namespace Zamza.Consumer.Models;

public sealed class ZamzaMessage<TKey, TValue>
{
    public string Topic { get; private set; }
    
    public int Partition { get; private set; }
    
    public long Offset { get; private set; }
    
    public IReadOnlyDictionary<string, byte[]> Headers { get; private set; }
    
    public TKey Key { get; private set; }
    
    public TValue Value { get; private set; }
    
    public Timestamp Timestamp { get; private set; }
    
    public int RetriesCount { get; private set; }

    public bool IsFromKafka => RetriesCount == 0;
    
    internal int MaxRetries { get; private set; }
    
    internal TimeSpan MinRetriesGap { get; private set; }
    
    internal TimeSpan? ProcessingPeriod { get; private set; }

    internal ZamzaMessage(
        string topic,
        int partition,
        long offset,
        IReadOnlyDictionary<string, byte[]> headers,
        TKey key,
        TValue value,
        Timestamp timestamp,
        int retriesCount,
        int maxRetries,
        TimeSpan minRetriesGap,
        TimeSpan? processingPeriod)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Headers = headers;
        Key = key;
        Value = value;
        Timestamp = timestamp;
        RetriesCount = retriesCount;
        MaxRetries = maxRetries;
        MinRetriesGap = minRetriesGap;
        ProcessingPeriod = processingPeriod;
    }

    public void IncreaseRetriesCount()
    {
        RetriesCount++;
    }
}