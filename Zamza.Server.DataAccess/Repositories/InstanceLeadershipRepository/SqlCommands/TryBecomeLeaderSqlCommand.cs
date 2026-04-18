using Dapper;

namespace Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository.SqlCommands;

internal sealed class TryBecomeLeaderSqlCommand
{
    private const int TimeoutInSeconds = 5;
    private const string Sql =
    $"""
        insert into zamza.instance_leadership
        (
            key,
            instance_id,
            leadership_deadline
        )
        values
        (
            @{nameof(Parameters.Key)},
            @{nameof(Parameters.InstanceId)},
            now() + (@{nameof(Parameters.LeadershipPeriodMs)} || ' milliseconds')::interval
        )
        on conflict (key)
        do update set
            instance_id = @{nameof(Parameters.InstanceId)},
            leadership_deadline = excluded.leadership_deadline
        where
            instance_id = @{nameof(Parameters.InstanceId)} or
            leadership_deadline < now()
        returning key;
    """;

    public static CommandDefinition BuildCommandDefinition(
        string key,
        string instanceId,
        long leadershipPeriodMs,
        CancellationToken cancellationToken)
    {
        var parameters = new Parameters(
            key,
            instanceId,
            leadershipPeriodMs);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            commandTimeout:  TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private sealed record Parameters(
        string Key,
        string InstanceId,
        long LeadershipPeriodMs);
}