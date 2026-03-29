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
        IReadOnlyCollection<MessageToDelete> messages,
        CancellationToken cancellationToken)
    {
        var topicValues = new string[messages.Count];
        var partitionValues = new int[messages.Count];
        var offsetValues = new long[messages.Count];

        var index = 0;
        foreach (var message in messages)
        {
            topicValues[index] = message.Topic;
            partitionValues[index] = message.Partition;
            offsetValues[index] = message.Offset;
            index++;
        }
        
        var command = DeleteDLQMessagesSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            consumerGroup,
            topicValues,
            partitionValues,
            offsetValues,
            cancellationToken);

        await transaction.Connection.ExecuteAsync(command);
    }

    public async Task Upsert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<FailedMessage> messages,
        CancellationToken cancellationToken)
    {
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