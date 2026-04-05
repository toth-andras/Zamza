using Dapper;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;

internal static class DeleteConsumerOwnershipsSqlCommand
{
    private const int TimeoutInSeconds = 5;

    private static string Sql =>
    $"""
        update zamza.partition_ownership
        set 
            consumer_id = null,
            timestamp_utc = @{nameof(Parameters.TimestampUtc)}
        where
            consumer_group = @{nameof(Parameters.ConsumerGroup)} and
            consumer_id = @{nameof(Parameters.ConsumerId)};
    """;

    public static CommandDefinition BuildCommandDefinition(
        string consumerId,
        string consumerGroup,
        DateTimeOffset timestampUtc,
        CancellationToken cancellationToken)
    {
        var parameters = new Parameters(
            consumerId,
            consumerGroup,
            timestampUtc);
        
        return new CommandDefinition(
            commandText: Sql,
            parameters: parameters,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private sealed record Parameters(
        string ConsumerId,
        string ConsumerGroup,
        DateTimeOffset TimestampUtc);
}