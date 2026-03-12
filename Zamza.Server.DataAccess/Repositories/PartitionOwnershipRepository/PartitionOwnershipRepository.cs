using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;
using Zamza.Server.DataAccess.Utils.DateTimeProvider;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

internal sealed class PartitionOwnershipRepository : IPartitionOwnershipRepository
{
    private readonly IDbConnectionsManager  _dbConnectionsManager;
    private readonly IDateTimeProvider _dateTimeProvider;

    public PartitionOwnershipRepository(
        IDbConnectionsManager dbConnectionsManager,
        IDateTimeProvider dateTimeProvider)
    {
        _dbConnectionsManager = dbConnectionsManager;
        _dateTimeProvider = dateTimeProvider;
    }

    public async Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        CancellationToken cancellation)
    {
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellation);
        
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
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellation);
        
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

    public async Task<CheckPartitionsOwnershipsRelevanceResponse> CheckPartitionsOwnershipsRelevance(
        ConsumerPartitionOwnershipsSet ownershipsToCheck,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        
        var sqlCommand = GetPartitionOwnershipsForConsumerGroupSqlCommand.BuildCommandDefinition(
            ownershipsToCheck.ConsumerGroup,
            cancellationToken);
        
        var ownershipsFromDb = (await connection.QueryAsync<PartitionOwnership>(sqlCommand)).ToList();

        var validatedOwnerships = 0;
        var allEpochsRelevant = true;
        for (var i = 0; i < ownershipsFromDb.Count; i++)
        {
            if (ownershipsToCheck.TryGetOwnershipEpochForPartition(
                    ownershipsFromDb[i].Topic,
                    ownershipsFromDb[i].Partition,
                    out var consumerOwnershipEpoch))
            {
                if (consumerOwnershipEpoch != ownershipsFromDb[i].Epoch)
                {
                    allEpochsRelevant = false;
                    break;
                }
                validatedOwnerships++;
            }
        }
        
        var isOwnershipRelevant = allEpochsRelevant
            && validatedOwnerships == ownershipsToCheck.Count; // = All consumer partition ownerships were validated
        
        return new CheckPartitionsOwnershipsRelevanceResponse(
            isOwnershipRelevant,
            ownershipsFromDb);
    }

    public async Task LockPartitions(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyList<TopicPartition> requestedPartitions,
        CancellationToken cancellationToken)
    {
        if (requestedPartitions.Count == 0)
        {
            return;
        }
        
        var topics = new string[requestedPartitions.Count];
        var partitions = new int[requestedPartitions.Count];
        for (var partitionIndex = 0; partitionIndex < requestedPartitions.Count; partitionIndex++)
        {
            topics[partitionIndex] = requestedPartitions[partitionIndex].Topic;
            partitions[partitionIndex] = requestedPartitions[partitionIndex].Partition;
        }

        var command = LockPartitionsSqlCommand.BuildCommandDefinition(
            transaction,
            consumerGroup,
            topics,
            partitions,
            cancellationToken);

        await transaction.Connection.ExecuteAsync(command);
    }

    public async Task LockPartitions(
        IDbTransactionFrame transaction,
        ConsumerPartitionOwnershipsSet partitionOwnerships,
        CancellationToken cancellationToken)
    {
        if (partitionOwnerships.Count == 0)
        {
            return;
        }

        var (topics, partitions) = partitionOwnerships.GetTopicsAndPartitions();

        var command = LockPartitionsSqlCommand.BuildCommandDefinition(
            transaction,
            partitionOwnerships.ConsumerGroup,
            topics,
            partitions,
            cancellationToken);
        
        await transaction.Connection.ExecuteAsync(command);
    }

    public async Task StopConsumerLeaderships(
        string consumerId,
        string consumerGroup,
        CancellationToken cancellation)
    {
        var command = StopConsumerLeadershipSqlCommand.BuildCommandDefinition(
            consumerId,
            consumerGroup,
            _dateTimeProvider.UtcNow,
            cancellation);
        
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellation);
        await connection.ExecuteAsync(command);
    }
}