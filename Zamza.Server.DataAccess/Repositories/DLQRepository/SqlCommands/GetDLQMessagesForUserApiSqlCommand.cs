using Dapper;
using Zamza.Server.DataAccess.Repositories.DLQRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository.SqlCommands;

internal static class GetDLQMessagesForUserApiSqlCommand
{
    private const int TimeoutInSeconds = 5;
    
    private static string Sql =>
    $"""
        select
           id               as {nameof(UserApiDLQMessageDto.Id)},
           consumer_group   as {nameof(UserApiDLQMessageDto.ConsumerGroup)},
           topic            as {nameof(UserApiDLQMessageDto.Topic)},
           partition        as {nameof(UserApiDLQMessageDto.Partition)},
           offset_value     as {nameof(UserApiDLQMessageDto.Offset)},
           headers          as {nameof(UserApiDLQMessageDto.HeadersJson)},
           key              as {nameof(UserApiDLQMessageDto.Key)},
           value            as {nameof(UserApiDLQMessageDto.Value)},
           timestamp        as {nameof(UserApiDLQMessageDto.Timestamp)},
           retries_count    as {nameof(UserApiDLQMessageDto.RetriesCount)},
           failed_at_utc    as {nameof(UserApiDLQMessageDto.SavedToDLQAtUTC)}
        from zamza.dlq
        where id > @{nameof(Parameters.StartId)}
        order by id
        limit @{nameof(Parameters.Limit)};
    """;

    public static CommandDefinition BuildCommandDefinition(
        long startId,
        int limit,
        CancellationToken cancellationToken)
    {
        var parameters = new Parameters(startId, limit);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private sealed record Parameters(
        long StartId,
        int Limit);
}