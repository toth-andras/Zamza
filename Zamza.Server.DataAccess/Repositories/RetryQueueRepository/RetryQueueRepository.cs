using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;
using Zamza.Server.DataAccess.Utils.DateTimeProvider;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

internal sealed class RetryQueueRepository : IRetryQueueRepository
{
    private readonly IDbConnectionsManager _dbConnectionsManager;
    private readonly IDateTimeProvider  _dateTimeProvider;

    public RetryQueueRepository(
        IDbConnectionsManager dbConnectionsManager, 
        IDateTimeProvider dateTimeProvider)
    {
        _dbConnectionsManager = dbConnectionsManager;
        _dateTimeProvider = dateTimeProvider;
    }
    
    public async Task<List<ConsumerApiMessage>> GetForFetch(
        string consumerGroup, 
        IReadOnlyList<PartitionFetch> fetchedPartitions, 
        int limit, 
        CancellationToken cancellationToken)
    {
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        
        var topics = new string[fetchedPartitions.Count];
        var partitions = new int[fetchedPartitions.Count];
        var kafkaOffsets = new long[fetchedPartitions.Count];

        for (var fetchIndex = 0; fetchIndex < fetchedPartitions.Count; fetchIndex++)
        {
            topics[fetchIndex] = fetchedPartitions[fetchIndex].Topic;
            partitions[fetchIndex] = fetchedPartitions[fetchIndex].Partition;
            kafkaOffsets[fetchIndex] = fetchedPartitions[fetchIndex].KafkaOffset;
        }

        var sqlCommand = GetMessagesForFetchSqlCommand.BuildCommandDefinition(
            consumerGroup,
            topics,
            partitions,
            kafkaOffsets,
            limit,
            cancellationToken);

        return (await connection.QueryAsync<ConsumerApiMessage>(sqlCommand)).ToList();
    }

    public async Task ClearMessages(
        IDbTransactionFrame transaction,
        MessageKeysSet messages,
        CancellationToken cancellationToken)
    {
        var command = ClearRetryMessagesSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            messages.ConsumerGroup,
            messages.TopicValue,
            messages.PartitionValue,
            messages.OffsetValue,
            cancellationToken);
        
        await transaction.Connection.ExecuteAsync(command);
    }

    public async Task Insert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyList<FailedMessage> messages,
        CancellationToken cancellationToken)
    {
        var topicValues = new string[messages.Count];
        var partitionValues = new int[messages.Count];
        var offsetValues = new long[messages.Count];
        var headersValues = new Dictionary<string, byte[]>[messages.Count];
        var keyValues = new byte[]?[messages.Count];
        var values = new byte[]?[messages.Count];
        var timestampValues = new DateTimeOffset[messages.Count];
        var maxRetriesValues = new int[messages.Count];
        var minRetriesMsValues = new long[messages.Count];
        var processingPeriodMsValues = new long?[messages.Count];
        var retriesCountValues = new int[messages.Count];
        var nextRetryAfterValues = new long[messages.Count];
        var lastRetryAtUtcValues = new DateTimeOffset[messages.Count];
        var utcNow = _dateTimeProvider.UtcNow;
        
        for (var index = 0; index < messages.Count; index++)
        {
            topicValues[index] = messages[index].Message.Topic;
            partitionValues[index] = messages[index].Message.Partition;
            offsetValues[index] = messages[index].Message.Offset;
            headersValues[index] = messages[index].Message.Headers;
            keyValues[index] = messages[index].Message.Key;
            values[index] = messages[index].Message.Value;
            timestampValues[index] = messages[index].Message.Timestamp;
            maxRetriesValues[index] = messages[index].Message.MaxRetries;
            minRetriesMsValues[index] = messages[index].Message.MinRetriesGapMs;
            processingPeriodMsValues[index] = messages[index].Message.ProcessingPeriodMs;
            retriesCountValues[index] = messages[index].Message.RetriesCount;
            nextRetryAfterValues[index] = messages[index].NextRetryAfterMs;
            lastRetryAtUtcValues[index] = utcNow;
        }

        var command = InsertToRetryQueueSqlCommand.BuildCommandDefinition(
            transaction,
            consumerGroup,
            topicValues,
            partitionValues,
            offsetValues,
            headersValues,
            keyValues,
            values,
            timestampValues,
            maxRetriesValues,
            minRetriesMsValues,
            processingPeriodMsValues,
            retriesCountValues,
            nextRetryAfterValues,
            lastRetryAtUtcValues,
            cancellationToken);
        
        await transaction.Connection.ExecuteAsync(command);
    }
}