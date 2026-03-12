using Dapper;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class StopConsumerLeadershipSqlCommand
{
    private const int TimeoutInSeconds = 10;

    private const string Sql =
    """
        update zamza.partition_ownership
        set 
            consumer_id = null,
            timestamp_utc = @UtcTimestamp
        where 
            consumer_group = @ConsumerGroup and
            consumer_id = @ConsumerId;
    """;

    public static CommandDefinition BuildCommandDefinition(
        string consumerId,
        string consumerGroup,
        DateTimeOffset utcTimestamp,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new
            {
                ConsumerGroup = consumerGroup,
                ConsumerId = consumerId,
                UtcTimestamp = utcTimestamp
            },
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}