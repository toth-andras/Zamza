namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Models;

internal sealed class FetchedMessageDto
{
    public required string ConsumerGroup { get; init; }
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required string HeadersJson { get; init; }
    public byte[]? Key { get; init; }
    public byte[]? Value { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required int MaxRetriesCount { get; init; }
    public required int RetriesCount { get; init; }
    public DateTimeOffset? ProcessingDeadlineUtc { get; init; }
}