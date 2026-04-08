namespace Zamza.Server.DataAccess.Repositories.DLQRepository.Models;

internal sealed class UserApiDLQMessageDto
{
    public required long Id { get; init; }
    public required string ConsumerGroup { get; init; }
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required string HeadersJson { get; init; }
    public byte[] Key { get; init; }
    public byte[] Value { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required int RetriesCount { get; init; }
    public required DateTimeOffset SavedToDLQAtUTC { get; init; }
}