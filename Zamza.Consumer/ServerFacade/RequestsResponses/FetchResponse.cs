using Zamza.Consumer.Models;

namespace Zamza.Consumer.ServerFacade.RequestsResponses;

internal sealed record FetchResponse<TKey, TValue>(
    int StatusCode,
    IReadOnlyCollection<PartitionOwnership> PartitionOwnershipsForConsumerGroup,
    IReadOnlyCollection<ZamzaMessage<TKey, TValue>> Messages,
    IReadOnlySet<string>? ProhibitedTopics)
{
    public const int PartitionOwnershipObsolete = 1;
    public const int PermissionDenied = 2;
    public const int Ok = 3;
}