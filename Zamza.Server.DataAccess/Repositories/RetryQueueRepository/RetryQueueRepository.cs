using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Common.QueryExecution;
using Zamza.Server.DataAccess.Repositories.CommonModels;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Mapping;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.Models;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi.Commit;
using Zamza.Server.Models.ConsumerApi.Fetch;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

internal sealed class RetryQueueRepository : IRetryQueueRepository
{
    private readonly IDbConnectionsManager _connectionsManager;

    public RetryQueueRepository(IDbConnectionsManager connectionsManager)
    {
        _connectionsManager = connectionsManager;
    }

    public async Task<List<FetchedMessage>> GetForFetch(
        string consumerGroup,
        IReadOnlyCollection<FetchedPartition> partitions,
        int limit,
        CancellationToken cancellationToken)
    {
        if (partitions.Count == 0 || limit == 0)
        {
            return [];
        }
        
        var topicValues = new string[partitions.Count];
        var partitionValues = new int[partitions.Count];
        var kafkaOffsetsValues = new long[partitions.Count];

        var index = 0;
        foreach (var partition in partitions)
        {
            topicValues[index] = partition.Topic;
            partitionValues[index] = partition.Partition;
            kafkaOffsetsValues[index] = partition.KafkaOffset;
            index++;
        }

        var command = FetchRetryQueueMessagesSqlCommand.BuildCommandDefinition(
            consumerGroup,
            topicValues,
            partitionValues,
            kafkaOffsetsValues,
            limit,
            cancellationToken);

        await using var connection = await _connectionsManager.CreateConnection(cancellationToken);

        var messages = await connection.QueryAsync<FetchedMessageDto>(command);

        return messages
            .Select(message => message.ToModel())
            .ToList();
    }

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
        
        var command = DeleteRetryQueueMessagesSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            consumerGroup,
            messages,
            cancellationToken);
        
        await transaction.Connection.ExecuteWithExceptionHandling(command);
    }

    public async Task Upsert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyCollection<RetryableMessage> messages,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }
        
        var messageDtos = messages
            .Select(message => message.ToDto())
            .ToList();

        var command = UpsertRetryQueueMessagesSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            consumerGroup,
            messageDtos,
            cancellationToken);
        
        await transaction.Connection.ExecuteWithExceptionHandling(command);
    }
}