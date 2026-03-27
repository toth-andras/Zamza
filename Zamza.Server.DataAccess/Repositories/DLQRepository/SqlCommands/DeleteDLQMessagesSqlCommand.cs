using System.Data.Common;
using Dapper;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository.SqlCommands;

internal static class DeleteDLQMessagesSqlCommand
{
    private const int TimeoutInSeconds = 10;
    
    private const string Sql = 
    """
        delete from zamza.dlq stored_rows
        using unnest(@Topics, @Partitions, @Offsets) 
            as rows_to_delete(topic, partition, offset_value)
        where
            stored_rows.consumer_group = @ConsumerGroup and
            stored_rows.topic = rows_to_delete.topic and
            stored_rows.partition = rows_to_delete.partition and
            stored_rows.offset_value = rows_to_delete.offset_value;
    """;
    
    public static CommandDefinition BuildCommandDefinition(
        DbTransaction  transaction,
        string consumerGroup,
        string[] topics,
        int[] partitions,
        long[] offsets,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup,
                Topics = topics,
                Partitions = partitions,
                Offsets = offsets
            },
            transaction: transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}