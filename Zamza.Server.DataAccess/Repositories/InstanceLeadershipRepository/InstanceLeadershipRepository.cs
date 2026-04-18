using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.QueryExecution;
using Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository.SqlCommands;

namespace Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository;

internal sealed class InstanceLeadershipRepository : IInstanceLeadershipRepository
{
    private readonly IDbConnectionsManager _dbConnectionsManager;

    public InstanceLeadershipRepository(IDbConnectionsManager dbConnectionsManager)
    {
        _dbConnectionsManager = dbConnectionsManager;
    }

    public async Task<bool> TryBecomeLeader(
        string key,
        Guid instanceId,
        TimeSpan leadershipPeriod,
        CancellationToken cancellationToken)
    {
        var command = TryBecomeLeaderSqlCommand.BuildCommandDefinition(
            key,
            instanceId.ToString(),
            (long) leadershipPeriod.TotalMilliseconds,
            cancellationToken);
        
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        var result = await connection.QueryFirstOrDefaultWithExceptionHandling<string>(command);

        return result is not null;
    }
}