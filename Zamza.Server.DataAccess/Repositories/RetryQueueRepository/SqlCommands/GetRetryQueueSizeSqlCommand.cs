using Dapper;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;

internal sealed class GetRetryQueueSizeSqlCommand
{
    private const int TimeoutInSeconds = 5;
    private const string Sql = "select count(*) from zamza.retry_queue;";

    public static CommandDefinition BuildCommandDefinition(CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}