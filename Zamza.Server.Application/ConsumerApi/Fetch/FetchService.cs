using Zamza.Server.Application.ConsumerApi.Fetch.Models;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;
using Zamza.Server.Models.ConsumerApi.Common;
using Zamza.Server.Models.ConsumerApi.Monitoring;

namespace Zamza.Server.Application.ConsumerApi.Fetch;

internal sealed class FetchService : IFetchService
{
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;
    private readonly IRetryQueueRepository _retryQueueRepository;
    private readonly IConsumerHeartbeatRepository _consumerHeartbeatRepository;

    public FetchService(
        IPartitionOwnershipRepository partitionOwnershipRepository,
        IRetryQueueRepository retryQueueRepository,
        IConsumerHeartbeatRepository consumerHeartbeatRepository)
    {
        _partitionOwnershipRepository = partitionOwnershipRepository;
        _retryQueueRepository = retryQueueRepository;
        _consumerHeartbeatRepository = consumerHeartbeatRepository;
    }

    public async Task<FetchResponse> Fetch(FetchRequest request, CancellationToken cancellationToken)
    {
        await SaveConsumerHeartbeat(request, cancellationToken);
        
        var consumerPartitionOwnershipValidationResult = await ValidateConsumerPartitionOwnership(
            request,cancellationToken);

        if (consumerPartitionOwnershipValidationResult.IsOwnershipRelevant is false)
        {
            return FetchResponse.AsObsoleteOwnership(consumerPartitionOwnershipValidationResult.CurrentConsumerGroupOwnerships);
        }

        var messages = await _retryQueueRepository.GetForFetch(
            request.ConsumerGroup,
            request.Partitions,
            request.Limit,
            cancellationToken);
        
        return FetchResponse.AsOk(
            consumerPartitionOwnershipValidationResult.CurrentConsumerGroupOwnerships,
            messages);
    }

    private async Task SaveConsumerHeartbeat(FetchRequest request, CancellationToken cancellationToken)
    {
        var heartbeat = new ConsumerHeartbeat(
            request.ConsumerId,
            request.ConsumerGroup,
            request.TimestampUtc);

        await _consumerHeartbeatRepository.SaveHeartbeat(heartbeat, cancellationToken);
    }

    private async Task<(bool IsOwnershipRelevant, ConsumerGroupPartitionOwnershipSet CurrentConsumerGroupOwnerships)> ValidateConsumerPartitionOwnership(
        FetchRequest request,
        CancellationToken cancellationToken)
    {
        var currentConsumerGroupOwnerships = await _partitionOwnershipRepository.GetForConsumerGroup(
            request.ConsumerGroup,
            cancellationToken);

        foreach (var fetchedPartition in request.Partitions)
        {
            var fetchingFromNotRegisteredPartition =
                currentConsumerGroupOwnerships.IsPartitionRegistered(fetchedPartition.Topic, fetchedPartition.Partition) is false;

            var fetchingWithObsoleteOwnership = 
                fetchedPartition.OwnershipEpoch != currentConsumerGroupOwnerships.GetOwnerEpochForPartition(fetchedPartition.Topic, fetchedPartition.Partition);
            
            if (fetchingFromNotRegisteredPartition || fetchingWithObsoleteOwnership)
            {
                return (false, currentConsumerGroupOwnerships);
            }
        }
        
        return (true, currentConsumerGroupOwnerships);
    }
}