using System.Data.Common;
using Dapper;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class LockPartitionsSqlCommand
{
    private const int TimeoutInSeconds = 10;

    private static string Sql =>
    $"""
        do
        $$
            declare topic_partition record;
            begin
                for topic_partition in
                    select 
                        topic,
                        partition
                    from unnest
                    (
                        @{nameof(Parameters.Topics)},
                        @{nameof(Parameters.Partitions)}
                    ) as claimed_partitions (topic, partition)
                    order by topic, partition
                loop
                    perform pg_advisory_xact_lock(
                        hashtextextended(@{nameof(Parameters.ConsumerGroup)} || ';' || topic_partition.topic || ';' || topic_partition.partition::text, 0)
                    );
                end loop;
            end;
        $$;
    """;

    public static CommandDefinition BuildCommandDefinition(
        DbTransaction  transaction,
        string consumerGroup,
        IReadOnlyCollection<PartitionToLock> partitions,
        CancellationToken cancellationToken)
    {
        var parameters = FormParameters(consumerGroup, partitions);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            transaction: transaction,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private static Parameters FormParameters(
        string consumerGroup,
        IReadOnlyCollection<PartitionToLock> partitionsToLock)
    {
        var topics = new string[partitionsToLock.Count];
        var partitions = new int[partitionsToLock.Count];

        var index = 0;
        foreach (var partition in partitionsToLock)
        {
            topics[index] = partition.Topic;
            partitions[index] = partition.Partition;
            index++;
        }
        
        return new Parameters(
            consumerGroup,
            topics,
            partitions);
    }
    
    private sealed record Parameters(
        string ConsumerGroup,
        string[] Topics,
        int[] Partitions);
}