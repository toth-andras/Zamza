using Dapper;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class GetPartitionOwnershipsForConsumerGroupSqlCommand
{
    private const int CommandTimeoutSeconds = 10;
    
    private const string Sql =
    $"""
        select
           topic        as {nameof(PartitionOwnership.Topic)},
           partition    as {nameof(PartitionOwnership.Partition)},
           epoch        as {nameof(PartitionOwnership.Epoch)},
           consumer_id  as {nameof(PartitionOwnership.ConsumerId)},
           timestamp    as {nameof(PartitionOwnership.Timestamp)}
        from zamza.partition_ownership
        where consumer_group = @ConsumerGroup;
    """;

    public static CommandDefinition BuildCommandDefinition(
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup
            },
            commandTimeout: CommandTimeoutSeconds,
            cancellationToken: cancellationToken);
    }
}