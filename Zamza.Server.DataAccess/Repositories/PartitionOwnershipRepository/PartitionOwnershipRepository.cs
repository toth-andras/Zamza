using Dapper;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
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
    
    public async Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        IDbTransactionFrame transaction,
        CancellationToken cancellation)
    {
        var sqlCommand = GetPartitionOwnershipsForConsumerGroupSqlCommand.BuildCommandDefinition(
            consumerGroup,
            cancellation);

        return (await transaction.Connection.QueryAsync<PartitionOwnership>(sqlCommand)).ToDictionary(
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

    public async Task LockPartitions(
        IDbTransactionFrame transaction, 
        PartitionOwnershipClaimsSet partitionsSource,
        CancellationToken cancellationToken)
    {
        var (topics, partitions, _) =  partitionsSource.ToDataArrays();

        var command = LockPartitionsSqlCommand.BuildCommandDefinition(
            transaction,
            partitionsSource.ConsumerGroup,
            topics,
            partitions,
            cancellationToken);
        
        await transaction.Connection.ExecuteAsync(command);
    }

    public async Task Insert(
        IDbTransactionFrame transaction,
        string consumerGroup,
        int ownershipsCount,
        IEnumerable<PartitionOwnership> consumerGroupOwnerships, CancellationToken cancellation)
    {
        var topics = new string[ownershipsCount];
        var partitions = new int[ownershipsCount];
        var epochs = new long[ownershipsCount];
        var consumerIds = new string[ownershipsCount];

        var index = 0;
        foreach (var ownership in consumerGroupOwnerships)
        {
            topics[index] = ownership.Topic;
            partitions[index] = ownership.Partition;
            epochs[index] = ownership.Epoch;
            consumerIds[index] = ownership.ConsumerId!;
            index++;
        }
        
        var command = InsertPartitionOwnershipsSqlCommand.BuildCommandDefinition(
            transaction,
            consumerGroup,
            topics,
            partitions,
            epochs,
            consumerIds,
            _dateTimeProvider.UtcNow,
            cancellation);
        
        await transaction.Connection.ExecuteAsync(command);
    }
}