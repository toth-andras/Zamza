using Dapper;

namespace Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository.SqlCommands;

internal sealed class DeleteConsumerSqlCommand
{
    private const int TimeoutInSeconds = 5;
    private const string Sql = 
    $"""
        delete from zamza.consumer_heartbeat
        where
            consumer_group = {nameof(Parameters.ConsumerGroup)} and
            consumer_id = {nameof(Parameters.ConsumerId)};
    """;

    public static CommandDefinition BuildCommandDefinition(
        string consumerGroup,
        string consumerId,
        CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            parameters: new Parameters(consumerGroup, consumerId),
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }

    private sealed record Parameters(
        string ConsumerGroup,
        string ConsumerId);
}