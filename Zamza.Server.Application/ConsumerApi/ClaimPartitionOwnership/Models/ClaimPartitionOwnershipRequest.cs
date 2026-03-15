using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;

public sealed record ClaimPartitionOwnershipRequest(
    string? BearerToken,
    PartitionOwnershipClaimsSet Claims);