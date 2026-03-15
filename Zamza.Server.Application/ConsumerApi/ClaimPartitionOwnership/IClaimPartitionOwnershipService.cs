using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;

namespace Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;

public interface IClaimPartitionOwnershipService
{
    Task<ClaimPartitionOwnershipResponse> ClaimPartitionOwnership(
        ClaimPartitionOwnershipRequest request,
        CancellationToken cancellationToken);
}