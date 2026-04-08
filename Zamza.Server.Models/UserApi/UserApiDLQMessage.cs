namespace Zamza.Server.Models.UserApi;

public sealed record UserApiDLQMessage(
    long Id,
    string ConsumerGroup,
    string Topic,
    int Partition,
    long Offset,
    IReadOnlyDictionary<string, byte[]> Headers,
    byte[]? Key,
    byte[]? Value,
    DateTimeOffset Timestamp,
    int RetriesCount,
    DateTimeOffset SavedToDLQAtUTC);