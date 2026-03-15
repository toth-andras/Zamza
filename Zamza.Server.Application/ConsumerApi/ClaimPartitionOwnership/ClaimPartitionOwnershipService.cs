using System.Data;
using Microsoft.Extensions.Logging;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.ConnectionsManagement.Transactions;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;

internal sealed class ClaimPartitionOwnershipService : IClaimPartitionOwnershipService
{
    private readonly IDbConnectionsManager _dbConnectionsManager;
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;
    private readonly ILogger<ClaimPartitionOwnershipService> _logger;

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
        var prohibitedTopics = await GetProhibitedTopics(
            request.Claims,
            cancellationToken);

        if (prohibitedTopics.Count > 0)
        {
            var currentOwnerships = await _partitionOwnershipRepository.Get(
                request.Claims.ConsumerGroup,
                cancellationToken);
            
            return ClaimPartitionOwnershipResponse.AsPermissionDenied(
                currentOwnerships.Values ,
                prohibitedTopics);
        }
        
        await using var transaction = await _dbConnectionsManager.BeginTransaction(
            IsolationLevel.ReadCommitted,
            cancellationToken);

        await _partitionOwnershipRepository.LockPartitions(
            transaction,
            request.Claims,
            cancellationToken);
        
        var knownOwnershipEpochRelevanceCheckResult = await CheckKnownOwnershipEpochRelevance(
            transaction,
            request.Claims,
            cancellationToken);

        if (knownOwnershipEpochRelevanceCheckResult.KnownEpochsAreRelevant is false)
        {
            await transaction.Commit(cancellationToken);
            return ClaimPartitionOwnershipResponse.AsPartitionOwnershipObsolete(
                knownOwnershipEpochRelevanceCheckResult.ConsumerGroupPartitionOwnerships.Values);
        }
        
        var consumerGroupOwnerships =
            knownOwnershipEpochRelevanceCheckResult.ConsumerGroupPartitionOwnerships.ToDictionary();
        foreach (var claim in request.Claims)
        {
            if (consumerGroupOwnerships.TryGetValue(claim.Partition, out var partitionOwnershipOwnership))
            {
                partitionOwnershipOwnership.SetNewOwner(request.Claims.ConsumerId, DateTimeOffset.UtcNow);
            }
            else
            {
                consumerGroupOwnerships[claim.Partition] = PartitionOwnership.CreateNew(
                    claim.Partition,
                    request.Claims.ConsumerId,
                    DateTimeOffset.UtcNow);
            }
        }
        
        await _partitionOwnershipRepository.Insert(
            transaction,
            request.Claims.ConsumerGroup,
            consumerGroupOwnerships.Count,
            consumerGroupOwnerships.Values,
            cancellationToken);
        
        await transaction.Commit(cancellationToken);
        
        return ClaimPartitionOwnershipResponse.AsOk(consumerGroupOwnerships.Values);
    }

    private async Task<HashSet<string>> GetProhibitedTopics(
        PartitionOwnershipClaimsSet claims,
        CancellationToken cancellationToken)
    {
        return new HashSet<string>();
    }

    private async Task<(bool KnownEpochsAreRelevant, IReadOnlyDictionary<(string Topic, int Partition),PartitionOwnership> ConsumerGroupPartitionOwnerships)> 
        CheckKnownOwnershipEpochRelevance(
            IDbTransactionFrame transaction,
            PartitionOwnershipClaimsSet claims, 
            CancellationToken cancellationToken)
    {
        var partitionOwnershipsFromDb = await _partitionOwnershipRepository.Get(
            claims.ConsumerGroup,
            transaction,
            cancellationToken);

        var knownEpochsAreRelevant = true;
        foreach (var ownershipClaim in claims)
        {
            if (partitionOwnershipsFromDb.TryGetValue(ownershipClaim.Partition, out var ownershipFromDb)
                && ownershipClaim.KnownOwnershipEpoch != ownershipFromDb.Epoch)
            {
                knownEpochsAreRelevant = false;
                break;
            }
        }
        
        return 
        (
            KnownEpochsAreRelevant: knownEpochsAreRelevant,
            ConsumerGroupPartitionOwnerships: partitionOwnershipsFromDb
        );
    }
}