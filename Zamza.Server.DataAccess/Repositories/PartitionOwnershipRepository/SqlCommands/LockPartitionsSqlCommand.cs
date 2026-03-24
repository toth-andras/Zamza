using System.Data.Common;
using Dapper;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class LockPartitionsSqlCommand
{
    private const int TimeoutInSeconds = 10;

    private const string Sql =
    """
        select
            pg_advisory_xact_lock(
                hashtextextended(@ConsumerGroup || ';' || topic || ';' || partition::text, 0)
            )
        from unnest(@Topics, @Partitions) as requested_partitions(topic, partition)
        order by topic, partition;                       
    """;

    public static CommandDefinition BuildCommandDefinition(
        DbTransaction  transaction,
        string consumerGroup,
        string[] topics,
        int[] partitions,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup,
                Topics = topics,
                Partitions = partitions
            },
            transaction: transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}