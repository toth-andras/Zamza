using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Common.QueryExecution;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.DataAccess.Repositories.DLQRepository.Mapping;
using Zamza.Server.DataAccess.Repositories.DLQRepository.Models;
using Zamza.Server.DataAccess.Repositories.DLQRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.UserApi;

namespace Zamza.Server.DataAccess.Repositories.DLQRepository;

internal sealed class DLQRepository : IDLQRepository
{
    private readonly IDbConnectionsManager _dbConnectionsManager;

    public DLQRepository(IDbConnectionsManager dbConnectionsManager)
    {
        _dbConnectionsManager = dbConnectionsManager;
    }

    public async Task Delete(
        IDbTransactionFrame transaction,
        MessageToDelete message,
        CancellationToken cancellationToken)
    {
        var command = DeleteDLQMessageSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            message.Topic,
            message.Partition,
            message.Offset,
            cancellationToken);

        await transaction.Connection.ExecuteWithExceptionHandling(command);
    }

    public async Task Delete(
        string consumerGroup,
        IReadOnlyCollection<MessageToDelete> messagesToDelete,
        CancellationToken cancellationToken)
    {
        if (messagesToDelete.Count == 0)
        {
            return;
        }

        var command = DeleteDLQMessagesForConsumerGroupSqlCommand.BuildCommandDefinition(
            transaction: null,
            consumerGroup,
            messagesToDelete,
            cancellationToken);

        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        await connection.ExecuteWithExceptionHandling(command);
    }

    public async Task Delete(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<MessageToDelete> messages,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }
        
        var command = DeleteDLQMessagesForConsumerGroupSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            consumerGroup,
            messages,
            cancellationToken);

        await transaction.Connection.ExecuteWithExceptionHandling(command);
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
        
        await transaction.Connection.ExecuteWithExceptionHandling(command);
    }

    public async Task<List<UserApiDLQMessage>> Get(
        long startId,
        int limit,
        CancellationToken cancellationToken)
    {
        var command = GetDLQMessagesForUserApiSqlCommand.BuildCommandDefinition(
            startId,
            limit,
            cancellationToken);

        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        
        var messagesEnumerable = await connection.QueryWithExceptionHandling<UserApiDLQMessageDto>(command);

        return messagesEnumerable
            .Select(dto => dto.ToModel())
            .ToList();
    }
}