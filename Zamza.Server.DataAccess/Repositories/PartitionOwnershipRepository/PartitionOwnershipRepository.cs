using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Common.QueryExecution;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Mapping;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi.Common;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

internal sealed class PartitionOwnershipRepository : IPartitionOwnershipRepository
{
    private readonly IDbConnectionsManager  _dbConnectionsManager;

    public PartitionOwnershipRepository(IDbConnectionsManager dbConnectionsManager)
    {
        _dbConnectionsManager = dbConnectionsManager;
    }

    public async Task LockPartitions(
        IDbTransactionFrame transaction,
        string consumerGroup,
        IReadOnlyList<PartitionToLock> claimedPartitions,
        CancellationToken cancellationToken)
    {
        if (claimedPartitions.Count == 0)
        {
            return;
        }
        
        var command = LockPartitionsSqlCommand.BuildCommandDefinition(
            transaction.Transaction,
            consumerGroup,
            claimedPartitions,
            cancellationToken);

        await transaction.Connection.ExecuteWithExceptionHandling(command);
    }

    public async Task<ConsumerGroupPartitionOwnershipSet> GetForConsumerGroup(
        IDbTransactionFrame transaction,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var command = GetConsumerGroupPartitionOwnershipsSqlCommand.CreateCommandDefinition(
            consumerGroup,
            transaction.Transaction,
            cancellationToken);

        var partitionOwnerships = await transaction.Connection
            .QueryWithExceptionHandling<PartitionOwnershipDto>(command);
        
        return new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            partitionOwnerships.Select(ownership => ownership.ToModel()));
    }

    public async Task<ConsumerGroupPartitionOwnershipSet> GetForConsumerGroup(
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var command = GetConsumerGroupPartitionOwnershipsSqlCommand.CreateCommandDefinition(
            consumerGroup,
            transaction: null,
            cancellationToken);
        
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);

        var partitionOwnerships = await connection
            .QueryWithExceptionHandling<PartitionOwnershipDto>(command);
        
        return new ConsumerGroupPartitionOwnershipSet(
            consumerGroup,
            partitionOwnerships.Select(ownership => ownership.ToModel()));
    }

    public async Task Upsert(
        IDbTransactionFrame transaction,
        ConsumerGroupPartitionOwnershipSet partitionOwnerships,
        CancellationToken cancellationToken)
    {
        if (partitionOwnerships.PartitionCount == 0)
        {
            return;
        }
        
        var ownershipDtos = partitionOwnerships
            .Select(ownership => ownership.ToDto(partitionOwnerships.ConsumerGroup))
            .ToList();
        
        var topicValues = new string[ownershipDtos.Count];
        var partitionValues = new int[ownershipDtos.Count];
        var epochValues = new long[ownershipDtos.Count];
        var consumerIdValues = new string?[ownershipDtos.Count];
        var timestampUtcValues = new DateTimeOffset[ownershipDtos.Count];
        for (var i = 0; i < ownershipDtos.Count; i++)
        {
            topicValues[i] = ownershipDtos[i].Topic;
            partitionValues[i] = ownershipDtos[i].Partition;
            epochValues[i] = ownershipDtos[i].Epoch;
            consumerIdValues[i] = ownershipDtos[i].ConsumerId;
            timestampUtcValues[i] = ownershipDtos[i].TimestampUtc;
        }

        var command = UpsertPartitionOwnershipsSqlCommand.BuildCommandDefinition(
            partitionOwnerships.ConsumerGroup,
            topicValues,
            partitionValues,
            epochValues,
            consumerIdValues,
            timestampUtcValues,
            transaction.Transaction,
            cancellationToken);

        await transaction.Connection.ExecuteWithExceptionHandling(command);
    }

    public async Task DeleteConsumerOwnerships(
        string consumerId,
        string consumerGroup,
        DateTimeOffset timestampUtc,
        CancellationToken cancellationToken)
    {
        var command = DeleteConsumerOwnershipsSqlCommand.BuildCommandDefinition(
            consumerId,
            consumerGroup,
            timestampUtc,
            cancellationToken);
        
        await using var connection = await _dbConnectionsManager.CreateConnection(cancellationToken);
        await connection.ExecuteWithExceptionHandling(command);
    }
}