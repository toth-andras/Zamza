using Zamza.Server.Application.ConsumerApi.Fetch.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

namespace Zamza.Server.Application.ConsumerApi.Fetch;

internal sealed class FetchService : IFetchService
{
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;
    private readonly IRetryQueueRepository  _retryQueueRepository;

    public FetchService(
        IPartitionOwnershipRepository partitionOwnershipRepository, 
        IRetryQueueRepository retryQueueRepository)
    {
        _partitionOwnershipRepository = partitionOwnershipRepository;
        _retryQueueRepository = retryQueueRepository;
    }

    public async Task<FetchResponse> Fetch(
        FetchRequest request,
        CancellationToken cancellationToken)
    {
        var checkPartitionOwnershipRelevanceResult = await _partitionOwnershipRepository
            .CheckPartitionsOwnershipsRelevance(
                request.ConsumerGroup,
                request.PartitionFetches,
                cancellationToken);

        if (checkPartitionOwnershipRelevanceResult.IsOwnershipRelevant is false)
        {
            return FetchResponse.AsPartitionOwnershipObsolete(checkPartitionOwnershipRelevanceResult.CurrentOwnerships);
        }

        var messages = await _retryQueueRepository.GetForFetch(
            request.ConsumerGroup,
            request.PartitionFetches,
            request.Limit,
            cancellationToken);
        
        return FetchResponse.AsOk(
            checkPartitionOwnershipRelevanceResult.CurrentOwnerships,
            messages);
    }
}