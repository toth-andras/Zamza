using Dapper;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.SqlCommands;

internal sealed class UpsertConsumerHearbeatSqlCommand
{
    private const int TimeoutInSeconds = 5;
    private const string Sql =
    $"""
        insert into zamza.consumer_heartbeat
        (
            consumer_group,
            consumer_id,
            timestamp_utc
        )
        values
        (
            @{nameof(ConsumerHeartbeatDto.ConsumerGroup)},
            @{nameof(ConsumerHeartbeatDto.ConsumerId)},
            @{nameof(ConsumerHeartbeatDto.TimestampUtc)}
        )
        on conflict (consumer_group, consumer_id)
        do update set
            timestamp_utc = excluded.timestamp_utc;
    """;

    public static CommandDefinition BuildCommandDefinition(
        ConsumerHeartbeatDto heartbeat,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: heartbeat,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}