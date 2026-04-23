using Microsoft.Extensions.Logging;
using Zamza.Server.Application.ConsumerApi.Leave.Models;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

namespace Zamza.Server.Application.ConsumerApi.Leave;

internal sealed class LeaveService : ILeaveService
{
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;
    private readonly IConsumerHeartbeatRepository _consumerHeartbeatRepository;
    private readonly ILogger<LeaveService> _logger;

    public LeaveService(
        IPartitionOwnershipRepository partitionOwnershipRepository,
        IConsumerHeartbeatRepository consumerHeartbeatRepository,
        ILogger<LeaveService> logger)
    {
        _partitionOwnershipRepository = partitionOwnershipRepository;
        _consumerHeartbeatRepository = consumerHeartbeatRepository;
        _logger = logger;
    }

    public async Task<LeaveResponse> Leave(
        LeaveRequest request,
        CancellationToken cancellationToken)
    {
        await _partitionOwnershipRepository.DeleteConsumerOwnerships(
            request.ConsumerId,
            request.ConsumerGroup,
            request.TimestampUtc,
            cancellationToken);

        await _consumerHeartbeatRepository.DeleteConsumer(
            request.ConsumerId,
            request.ConsumerGroup,
            cancellationToken);
        
        _logger.LogInformation(
            "Consumer \'{ConsumerId}\' has left consumer group \'{ConsumerGroup}\'",
            request.ConsumerId,
            request.ConsumerGroup);
        
        return LeaveResponse.Instance;
    }
}