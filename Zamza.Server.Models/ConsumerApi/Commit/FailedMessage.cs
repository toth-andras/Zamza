using Zamza.Server.Models.Validators;

namespace Zamza.Server.Models.ConsumerApi.Commit;

public sealed record FailedMessage
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public IReadOnlyDictionary<string, byte[]> Headers { get; }
    public byte[]? Key { get; }
    public byte[]? Value { get; }
    public DateTimeOffset Timestamp { get; }
    public int RetriesCount { get; }
    public DateTimeOffset FailedAtUtc { get; }

    public FailedMessage(
        string topic,
        int partition,
        long offset,
        IReadOnlyDictionary<string, byte[]> headers,
        byte[]? key,
        byte[]? value,
        DateTimeOffset timestamp,
        int retriesCount,
        DateTimeOffset failedAtUtc)
    {
        ThrowBadRequest.IfEmpty(topic, "Failed message Topic");
        ThrowBadRequest.IfNull(headers, "Failed message Headers");
        ThrowBadRequest.IfNegative(retriesCount, "Failed message RetriesCount");
        ThrowIfNotUtc(failedAtUtc, "Failed message FailedAtUtc");
        
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Headers = headers;
        Key = key;
        Value = value;
        Timestamp = timestamp;
        RetriesCount = retriesCount;
        FailedAtUtc = failedAtUtc;
    }

    private void ThrowIfNotUtc(DateTimeOffset? timestamp, string paramName)
    {
        if (timestamp is null)
        {
            return;
        }

        ThrowBadRequest.IfNotUtc(timestamp.Value, paramName);
    }
}