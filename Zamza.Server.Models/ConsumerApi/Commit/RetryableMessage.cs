using Zamza.Server.Models.Exceptions;
using Zamza.Server.Models.Validators;

namespace Zamza.Server.Models.ConsumerApi.Commit;

public sealed record RetryableMessage
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public IReadOnlyDictionary<string, byte[]> Headers { get; }
    public byte[]? Key { get; }
    public byte[]? Value { get; }
    public DateTimeOffset Timestamp { get; }
    public int MaxRetriesCount { get; }
    public int RetriesCount { get; }
    public DateTimeOffset? ProcessingDeadlineUtc { get; }
    public long NextRetryAfterMs { get; }

    public RetryableMessage(
        string topic,
        int partition,
        long offset,
        IReadOnlyDictionary<string, byte[]> headers,
        byte[]? key,
        byte[]? value,
        DateTimeOffset timestamp,
        int maxRetriesCount,
        int retriesCount,
        DateTimeOffset? processingDeadlineUtc,
        long nextRetryAfterMs)
    {
        ThrowBadRequest.IfEmpty(topic, "Retryable message Topic");
        ThrowBadRequest.IfNull(headers, "Retryable message Headers");
        ThrowBadRequest.IfNotPositive(maxRetriesCount, "Retryable message MaxRetriesCount");
        ThrowBadRequest.IfNegative(retriesCount, "Retryable message RetriesCount");
        ThrowIfMaxRetriesNumberExceeded(retriesCount, maxRetriesCount);
        ThrowIfNotUtc(processingDeadlineUtc, "Retryable message ProcessingDeadlineUtc");
        ThrowBadRequest.IfNegative(nextRetryAfterMs, "Retryable message NextRetryAfterMs");
        
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Headers = headers;
        Key = key;
        Value = value;
        Timestamp = timestamp;
        MaxRetriesCount = maxRetriesCount;
        RetriesCount = retriesCount;
        ProcessingDeadlineUtc = processingDeadlineUtc;
        NextRetryAfterMs = nextRetryAfterMs;
    }

    private static void ThrowIfNotUtc(DateTimeOffset? timestamp, string paramName)
    {
        if (timestamp is null)
        {
            return;
        }
        
        ThrowBadRequest.IfNotUtc(timestamp.Value, paramName);
    }

    private static void ThrowIfMaxRetriesNumberExceeded(
        int actualRetriesCount,
        int maxRetriesCount)
    {
        if (actualRetriesCount > maxRetriesCount)
        {
            throw new BadRequestException(
                "The message must be failed, not retryable, as the maximum number of retries exceeded.");
        }
    }
}