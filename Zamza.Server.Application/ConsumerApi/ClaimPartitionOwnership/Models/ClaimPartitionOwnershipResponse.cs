using Zamza.Server.Models.ConsumerApi.Common;

namespace Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;

public sealed record ClaimPartitionOwnershipResponse(
    ConsumerGroupPartitionOwnershipSet ConsumerGroupPartitionOwnerships,
    OwnershipClaimResult Result);