namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

internal sealed class PartitionOwnershipDto
{
    public required string ConsumerGroup { get; init; }
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Epoch { get; init; }
    public string? ConsumerId { get; init; }
    public required DateTimeOffset TimestampUtc { get; init; }
}