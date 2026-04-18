using Dapper;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository.SqlCommands;

internal sealed class GetDLQSizeSqlCommand
{
    private const int TimeoutInSeconds = 5;
    private const string Sql = "select count(*) from zamza.dlq;";

    public static CommandDefinition BuildCommandDefinition(CancellationToken cancellationToken)
    {
        return new CommandDefinition(
            commandText: Sql,
            commandTimeout: TimeoutInSeconds,
            cancellationToken: cancellationToken);
    }
}