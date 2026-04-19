using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.QueryExecution;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Mapping;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Models;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi.Monitoring;

namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;

internal sealed class ConsumerHeartbeatRepository : IConsumerHeartbeatRepository
{
    private readonly IDbConnectionsManager _dbConnectionsManager;

    public ConsumerHeartbeatRepository(IDbConnectionsManager dbConnectionsManager)
    {
        _dbConnectionsManager = dbConnectionsManager;
    }

    public async Task SaveHeartbeat(ConsumerHeartbeat heartbeat, CancellationToken cancellationToken)
    {
        var command = UpsertConsumerHearbeatSqlCommand.BuildCommandDefinition(
            heartbeat.ToDto(),
            cancellationToken);

        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        await connection.ExecuteWithExceptionHandling(command);
    }

    public async Task DeleteConsumer(
        string consumerId,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var command = DeleteConsumerSqlCommand.BuildCommandDefinition(
            consumerGroup,
            consumerId,
            cancellationToken);
        
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        await connection.ExecuteWithExceptionHandling(command);
    }

    public async Task<ZamzaConsumer[]> GetOfflineConsumers(
        DateTimeOffset lastHeartbeatEarlierThanUtc,
        CancellationToken cancellationToken)
    {
        var command = GetOfflineConsumersSqlCommand.BuildCommandDefinition(
            lastHeartbeatEarlierThanUtc,
            cancellationToken);
        
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        var heartbeats = await connection.QueryWithExceptionHandling<ConsumerHeartbeatDto>(command);
        
        return heartbeats
            .Select(heartbeat => new ZamzaConsumer(heartbeat.ConsumerGroup, heartbeat.ConsumerId))
            .ToArray();
    }
}