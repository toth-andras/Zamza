using Zamza.Consumer.Models;

namespace Zamza.Consumer.ServerFacade.RequestsResponses;

internal sealed record ClaimOwnershipResponse (
    int ResponseCode,
    IReadOnlyCollection<PartitionOwnership> ConsumerGroupOwnerships,
    IReadOnlyCollection<string>? ProhibitedTopics)
{
    public const int OwnershipObsolete = 1;
    public const int PermissionDenied = 2;
    public const int Ok = 3;
}