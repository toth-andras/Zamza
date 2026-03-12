using Microsoft.Extensions.Logging;
using Zamza.Server.Application.ConsumerApi.Leave.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

namespace Zamza.Server.Application.ConsumerApi.Leave;

internal sealed class LeaveService : ILeaveService
{
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;
    private readonly ILogger<LeaveService> _logger;

    public LeaveService(
        IPartitionOwnershipRepository partitionOwnershipRepository, 
        ILogger<LeaveService> logger)
    {
        _partitionOwnershipRepository = partitionOwnershipRepository;
        _logger = logger;
    }

    public async Task Leave(LeaveRequest request, CancellationToken cancellationToken)
    {
        await _partitionOwnershipRepository.StopConsumerLeaderships(
            request.ConsumerId,
            request.ConsumerGroup,
            cancellationToken);
        
        _logger.LogInformation(
            "Consumer {ConsumerId} has left consumer group {ConsumerGroup}",
            request.ConsumerId,
            request.ConsumerGroup);
    }
}