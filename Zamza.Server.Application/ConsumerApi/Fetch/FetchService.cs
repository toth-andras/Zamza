using Zamza.Server.Application.ConsumerApi.Fetch.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;
using Zamza.Server.Models.ConsumerApi;

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
        var partitionOwners = await _partitionOwnershipRepository.Get(
            request.ConsumerGroup,
            cancellationToken);

        if (IsOwnerForAllFetchedPartitions(request.PartitionFetches, partitionOwners) is false)
        {
            return FetchResponse.AsPartitionOwnershipObsolete(partitionOwners.Values);
        }

        var messages = await _retryQueueRepository.GetForFetch(
            request.ConsumerGroup,
            request.PartitionFetches,
            request.Limit,
            cancellationToken);
        
        return FetchResponse.AsOk(
            partitionOwners.Values,
            messages);
    }

    private static bool IsOwnerForAllFetchedPartitions(
        IReadOnlyCollection<PartitionFetch> partitions,
        IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership> partitionOwners)
    {
        foreach (var partitionFetch in partitions)
        {
            if (partitionOwners.TryGetValue((partitionFetch.Topic, partitionFetch.Partition), out var partitionOwner) 
                && partitionFetch.OwnershipEpoch != partitionOwner.Epoch)
            {
                return false;
            }
        }

        return true;
    }
}