using System.Data;
using Microsoft.Extensions.Logging;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;
using Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership;

namespace Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;

internal sealed class ClaimPartitionOwnershipService : IClaimPartitionOwnershipService
{
    private readonly IDbConnectionsManager  _dbConnectionsManager;
    private readonly IPartitionOwnershipRepository  _partitionOwnershipRepository;
    private readonly ILogger<ClaimPartitionOwnershipService>  _logger;

    public ClaimPartitionOwnershipService(
        IDbConnectionsManager dbConnectionsManager,
        IPartitionOwnershipRepository partitionOwnershipRepository,
        ILogger<ClaimPartitionOwnershipService> logger)
    {
        _dbConnectionsManager = dbConnectionsManager;
        _partitionOwnershipRepository = partitionOwnershipRepository;
        _logger = logger;
    }

    public async Task<ClaimPartitionOwnershipResponse> ClaimPartitionOwnership(
        ClaimPartitionOwnershipRequest request,
        CancellationToken cancellationToken)
    {
        await using var transaction = await _dbConnectionsManager.BeginTransaction(
            IsolationLevel.ReadCommitted,
            cancellationToken);

        await LockPartitions(
            transaction,
            request.PartitionClaims,
            cancellationToken);

        var consumerGroupPartitionOwnerships = await _partitionOwnershipRepository.GetForConsumerGroup(
            transaction,
            request.PartitionClaims.ConsumerGroup,
            cancellationToken);

        var claims = request.PartitionClaims;

        foreach (var partitionClaim in claims.Partitions)
        {
            if (partitionClaim.CurrentOwnerEpoch !=
                consumerGroupPartitionOwnerships.GetCurrentOwnerForPartition(partitionClaim.Topic, partitionClaim.Partition))
            {
                await transaction.Commit(cancellationToken);
                return new ClaimPartitionOwnershipResponse(
                    consumerGroupPartitionOwnerships,
                    OwnershipClaimResult.Obsolete);
            }  
            
            consumerGroupPartitionOwnerships.SetNewPartitionOwner(
                partitionClaim.Topic,
                partitionClaim.Partition,
                partitionClaim.CurrentOwnerEpoch,
                claims.ConsumerId,
                claims.TimestampUtc);
        }

        await _partitionOwnershipRepository.Upsert(
            transaction,
            consumerGroupPartitionOwnerships,
            cancellationToken);

        await transaction.Commit(cancellationToken);
        
        LogNewPartitionsOwnership(claims);
        
        return new ClaimPartitionOwnershipResponse(
            consumerGroupPartitionOwnerships,
            OwnershipClaimResult.Ok);
    }

    private async Task LockPartitions(
        IDbTransactionFrame transaction,
        PartitionOwnershipClaimSet claims,
        CancellationToken cancellationToken)
    {
        var partitionClaims = claims.Partitions
            .Select(partition => new PartitionToLock(partition.Topic, partition.Partition))
            .ToList();

        await _partitionOwnershipRepository.LockPartitions(
            transaction,
            claims.ConsumerGroup,
            partitionClaims,
            cancellationToken);
    }

    private void LogNewPartitionsOwnership(PartitionOwnershipClaimSet claims)
    {
        if (claims.Partitions.Count == 0)
        {
            return;
        }
        
        var topicPartitionPairs = claims.Partitions
            .Select(partition => (partition.Topic, partition.Partition));
        
        _logger.LogInformation(
            "Provided ownership for consumer {ConsumerId} in {ConsumerGroup} consumer group over partitions: {ProvidedPartitions}",
            claims.ConsumerId,
            claims.ConsumerGroup,
            topicPartitionPairs);
    }
}