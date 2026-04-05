using System.Data.Common;
using Dapper;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class LockPartitionsSqlCommand
{
    private const int TimeoutInSeconds = 10;

    private static string Sql =>
    $"""
        call zamza.lock_partitions(@{nameof(Parameters.ConsumerGroup)}, @{nameof(Parameters.Topics)}::text[], @{nameof(Parameters.Partitions)}::int[]);
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