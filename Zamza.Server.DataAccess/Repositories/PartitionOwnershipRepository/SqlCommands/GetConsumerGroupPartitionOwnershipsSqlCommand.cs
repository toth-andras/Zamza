using System.Data.Common;
using Dapper;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class GetConsumerGroupPartitionOwnershipsSqlCommand
{
    private const int TimeoutInSeconds = 10;
    
    private const string Sql = 
    $"""
        select
            topic           as {nameof(PartitionOwnershipDto.Topic)},
            partition       as {nameof(PartitionOwnershipDto.Partition)},
            epoch           as {nameof(PartitionOwnershipDto.Epoch)},
            consumer_id     as {nameof(PartitionOwnershipDto.ConsumerId)},
            timestamp_utc   as {nameof(PartitionOwnershipDto.TimestampUtc)}
        from zamza.partition_ownership
        where consumer_group = @ConsumerGroup;
    """;

    public static CommandDefinition CreateCommandDefinition(
        DbTransaction transaction,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup =  consumerGroup,
            },
            transaction: transaction,
            commandTimeout:  TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}