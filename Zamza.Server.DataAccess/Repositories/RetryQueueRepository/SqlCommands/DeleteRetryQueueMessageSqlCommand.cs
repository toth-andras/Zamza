using System.Data.Common;
using Dapper;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;

internal static class DeleteRetryQueueMessageSqlCommand
{
    private const int TimeoutInSeconds = 10;
    
    private static string Sql => 
    $"""
        delete from zamza.retry_queue
        where
            topic = @{nameof(Parameters.Topic)} and
            partition = @{nameof(Parameters.Partition)} and
            offset_value = @{nameof(Parameters.Offset)};
    """;

    public static CommandDefinition BuildCommandDefinition(
        DbTransaction transaction,
        string topic, 
        int partition,
        long offset,
        CancellationToken cancellationToken)
    {
        var parameters = new Parameters(topic, partition, offset);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            transaction: transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private sealed record Parameters(
        string Topic,
        int Partition,
        long Offset);
}