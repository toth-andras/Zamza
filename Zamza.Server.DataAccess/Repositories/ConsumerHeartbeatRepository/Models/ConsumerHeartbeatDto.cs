namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Models;

internal sealed class ConsumerHeartbeatDto
{
    public required string ConsumerId { get; init; }
    public required string ConsumerGroup { get; init; }
    public required DateTimeOffset TimestampUtc { get; init; }
}