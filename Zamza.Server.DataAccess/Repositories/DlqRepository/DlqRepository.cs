using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.DlqRepository.SqlCommands;
using Zamza.Server.DataAccess.Utils.DateTimeProvider;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.DlqRepository;

internal sealed class DlqRepository : IDlqRepository
{
    private readonly IDateTimeProvider  _dateTimeProvider;

    public DlqRepository(IDateTimeProvider dateTimeProvider)
    {
        _dateTimeProvider = dateTimeProvider;
    }

    public async Task ClearMessages(
        IDbTransactionFrame transaction,
        MessageKeysSet messagesToRemove,
        CancellationToken cancellationToken)
    {
        var command = ClearDlqMessagesSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            messagesToRemove.ConsumerGroup,
            messagesToRemove.TopicValue,
            messagesToRemove.PartitionValue,
            messagesToRemove.OffsetValue,
            cancellationToken);
        
        await transaction.Connection.ExecuteAsync(command);
    }

    public async Task Insert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyList<ConsumerApiMessage> messages,
        CancellationToken cancellationToken)
    {
        var topicValues = new string[messages.Count];
        var partitionValues =  new int[messages.Count];
        var offsetValues =  new long[messages.Count];
        var headersValues =  new Dictionary<string, byte[]>[messages.Count];
        var keyValues = new byte[]?[messages.Count];
        var values = new byte[]?[messages.Count];
        var timestampValues = new DateTimeOffset[messages.Count];
        var retriesCountValues = new int[messages.Count];

        for (int index = 0; index < messages.Count; index++)
        {
            topicValues[index] = messages[index].Topic;
            partitionValues[index] = messages[index].Partition;
            offsetValues[index] = messages[index].Offset;
            headersValues[index] = messages[index].Headers;
            keyValues[index] = messages[index].Key;
            values[index] = messages[index].Value;
            timestampValues[index] = messages[index].Timestamp;
            retriesCountValues[index] = messages[index].RetriesCount;
        }

        var command = InsertToDqlSqlCommand.BuildCommandDefinition(
            transaction,
            consumerGroup,
            topicValues,
            partitionValues,
            offsetValues,
            headersValues,
            keyValues,
            values,
            timestampValues,
            retriesCountValues,
            _dateTimeProvider.UtcNow,
            cancellationToken);
        
        await transaction.Connection.ExecuteAsync(command);
    }
}