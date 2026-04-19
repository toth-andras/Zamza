using Dapper;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.SqlCommands;

internal sealed class GetOfflineConsumersSqlCommand
{
    private const int TimeoutInSeconds = 10;
    private const string Sql =
    $"""
        select
            consumer_id     as {nameof(ConsumerHeartbeatDto.ConsumerId)},
            consumer_group  as {nameof(ConsumerHeartbeatDto.ConsumerGroup)},
            timestamp_utc   as {nameof(ConsumerHeartbeatDto.TimestampUtc)}
        from zamza.consumer_heartbeat
        where timestamp_utc < @{nameof(Parameters.LastHeartbeatEarlierThanUtc)};
    """;

    public static CommandDefinition BuildCommandDefinition(
        DateTimeOffset lastHeartbeatEarlierThanUtc,
        CancellationToken cancellationToken)
    {
        var parameters = new Parameters(lastHeartbeatEarlierThanUtc);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private sealed record Parameters(
        DateTimeOffset LastHeartbeatEarlierThanUtc);
}