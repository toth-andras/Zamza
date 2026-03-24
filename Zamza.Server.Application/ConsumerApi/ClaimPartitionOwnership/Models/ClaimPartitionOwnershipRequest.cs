using Zamza.Server.Models.ConsumerApi.ClaimPartitionOwnership;

namespace Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;

public sealed record ClaimPartitionOwnershipRequest(
    PartitionOwnershipClaimSet PartitionClaims);