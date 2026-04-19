namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Models;

public sealed record ZamzaConsumer(
    string ConsumerGroup,
    string ConsumerId);