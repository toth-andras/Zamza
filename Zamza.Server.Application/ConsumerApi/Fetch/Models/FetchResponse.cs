using OneOf;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Fetch.Models;

public sealed record FetchResponse(
    IEnumerable<PartitionOwnership> CurrentOwners,
    OneOf<FetchResponse.PartitionOwnershipObsolete, FetchResponse.PermissionDenied, FetchResponse.Ok> Result)
{
    public static FetchResponse AsPartitionOwnershipObsolete(IEnumerable<PartitionOwnership> currentOwners)
    {
        return new FetchResponse(
            currentOwners,
            OneOf<PartitionOwnershipObsolete, PermissionDenied, Ok>.FromT0(PartitionOwnershipObsolete.Instance));
    }

    public static FetchResponse AsPermissionDenied(
        IEnumerable<PartitionOwnership> currentOwners,
        IReadOnlyCollection<string> prohibitedTopics)
    {
        return new FetchResponse(
            currentOwners,
            OneOf<PartitionOwnershipObsolete, PermissionDenied, Ok>.FromT1(new PermissionDenied(prohibitedTopics)));
    }

    public static FetchResponse AsOk(
        IEnumerable<PartitionOwnership> currentOwners,
        IReadOnlyCollection<ConsumerApiMessage> messages)
    {
        return new FetchResponse(
            currentOwners,
            OneOf<PartitionOwnershipObsolete, PermissionDenied, Ok>.FromT2(new Ok(messages)));
    }
    
    public sealed record PartitionOwnershipObsolete
    {
        public static PartitionOwnershipObsolete Instance { get; } = new();
    }

    public sealed record PermissionDenied(
        IReadOnlyCollection<string> ProhibitedTopics);

    public sealed record Ok(
        IReadOnlyCollection<ConsumerApiMessage> Messages);
}