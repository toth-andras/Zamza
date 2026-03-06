using Dapper;
using Zamza.Server.DataAccess.Common.Connections;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

internal sealed class RetryQueueRepository : IRetryQueueRepository
{
    private readonly IConnectionFactory _connectionFactory;

    public RetryQueueRepository(IConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }
    
    public async Task<List<ConsumerApiMessage>> GetForFetch(
        string consumerGroup, 
        IReadOnlyList<PartitionFetch> fetchedPartitions, 
        int limit, 
        CancellationToken cancellationToken)
    {
        await using var connection = await _connectionFactory.CreateConnection(cancellationToken);
        
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
}