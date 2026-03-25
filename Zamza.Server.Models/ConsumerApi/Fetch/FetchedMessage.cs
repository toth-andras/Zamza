namespace Zamza.Server.Models.ConsumerApi.Fetch;

public sealed record FetchedMessage(
    string Topic,
    int Partition,
    long Offset,
    IReadOnlyDictionary<string, byte[]> Headers,
    byte[]? Key,
    byte[]? Value,
    DateTimeOffset Timestamp,
    int MaxRetriesCount,
    int RetriesCount,
    DateTimeOffset? ProcessingDeadlineUtc);