using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.DataAccess.Repositories.DLQRepository.Mapping;
using Zamza.Server.DataAccess.Repositories.DLQRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi.Commit;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository;

internal sealed class DLQRepository : IDLQRepository
{
    public async Task Delete(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<MessageToDeleteDto> messages,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }
        
        var command = DeleteDLQMessagesSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            consumerGroup,
            messages,
            cancellationToken);

        await transaction.Connection.ExecuteAsync(command);
    }

    public async Task Upsert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<FailedMessage> messages,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }
        
        var messageDtos = messages
            .Select(message => message.ToDto())
            .ToList();

        var command = UpsertDLQMessagesSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            consumerGroup,
            messageDtos,
            cancellationToken);
        
        await transaction.Connection.ExecuteAsync(command);
    } 
}