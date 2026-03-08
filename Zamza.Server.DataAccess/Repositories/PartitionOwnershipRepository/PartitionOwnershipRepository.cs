using Dapper;
using Zamza.Server.DataAccess.Common.Connections;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

internal sealed class PartitionOwnershipRepository : IPartitionOwnershipRepository
{
    private readonly IConnectionFactory  _connectionFactory;

    public PartitionOwnershipRepository(IConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        CancellationToken cancellation)
    {
        await using var connection = await _connectionFactory.CreateConnection(cancellation);
        
        var sqlCommand = GetPartitionOwnershipsForConsumerGroupSqlCommand.BuildCommandDefinition(
            consumerGroup,
            cancellation);

        return (await connection.QueryAsync<PartitionOwnership>(sqlCommand)).ToDictionary(
            partitionOwnership => (partitionOwnership.Topic, partitionOwnership.Partition),
            partitionOwnership => partitionOwnership);
    }

    public async Task<CheckPartitionsOwnershipsRelevanceResponse> CheckPartitionsOwnershipsRelevance(
        string consumerGroup, 
        IReadOnlyCollection<PartitionFetch> fetchesToCheck,
        CancellationToken cancellation)
    {
        await using var connection = await _connectionFactory.CreateConnection(cancellation);
        
        var sqlCommand = GetPartitionOwnershipsForConsumerGroupSqlCommand.BuildCommandDefinition(
            consumerGroup,
            cancellation);
        
        var partitionOwnerships = (await connection.QueryAsync<PartitionOwnership>(sqlCommand)).ToList();
        
        var partitionOwnershipsByPartitions = partitionOwnerships.ToDictionary(
            partitionOwnership => (partitionOwnership.Topic, partitionOwnership.Partition));

        var isOwnerForAllFetchedPartition = true;
        foreach (var fetch in fetchesToCheck)
        {
            var isPartitionOwner =
                partitionOwnershipsByPartitions.TryGetValue((fetch.Topic, fetch.Partition), out var partitionOwnership)
                && partitionOwnership.Epoch == fetch.OwnershipEpoch;

            if (isPartitionOwner is false)
            {
                isOwnerForAllFetchedPartition = false;
                break;
            }
        }

        return new CheckPartitionsOwnershipsRelevanceResponse(
            IsOwnershipRelevant: isOwnerForAllFetchedPartition,
            partitionOwnerships);
    }
}