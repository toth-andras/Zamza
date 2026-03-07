using Zamza.Server.Application.ConsumerApi.Ping.Models;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

namespace Zamza.Server.Application.ConsumerApi.Ping;

internal sealed class PingService : IPingService
{
    private readonly IPartitionOwnershipRepository _partitionOwnershipRepository;

    public PingService(IPartitionOwnershipRepository partitionOwnershipRepository)
    {
        _partitionOwnershipRepository = partitionOwnershipRepository;
    }

    public async Task<PingResponse> Ping(PingRequest request, CancellationToken cancellationToken)
    {
        var partitionOwners = await _partitionOwnershipRepository.Get(
            request.ConsumerGroup,
            cancellationToken);

        return new PingResponse(partitionOwners.Values);
    }
}