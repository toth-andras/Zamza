using OneOf;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership.Models;

public sealed record ClaimPartitionOwnershipResponse
{
    public IEnumerable<PartitionOwnership> CurrentPartitionOwnershipsForConsumerGroup { get; }
    public OneOf
    <
        ResultOneOfPartitionOwnershipObsolete, 
        ResultOneOfPermissionDenied,
        ResultOneOfOk
    > Result;

    private ClaimPartitionOwnershipResponse(
        IEnumerable<PartitionOwnership> currentPartitionOwnershipsForConsumerGroup,
        OneOf<ResultOneOfPartitionOwnershipObsolete, ResultOneOfPermissionDenied, ResultOneOfOk> result)
    {
        CurrentPartitionOwnershipsForConsumerGroup = currentPartitionOwnershipsForConsumerGroup;
        Result = result;
    }

    public static ClaimPartitionOwnershipResponse AsPartitionOwnershipObsolete(
        IEnumerable<PartitionOwnership> currentPartitionOwnershipsForConsumerGroup)
    {
        return new ClaimPartitionOwnershipResponse(
            currentPartitionOwnershipsForConsumerGroup,
            ResultOneOfPartitionOwnershipObsolete.Instance);
    }

    public static ClaimPartitionOwnershipResponse AsPermissionDenied(
        IEnumerable<PartitionOwnership> currentPartitionOwnershipsForConsumerGroup,
        IReadOnlyCollection<string> prohibitedTopics)
    {
        return new ClaimPartitionOwnershipResponse(
            currentPartitionOwnershipsForConsumerGroup,
            new ResultOneOfPermissionDenied(prohibitedTopics));
    }

    public static ClaimPartitionOwnershipResponse AsOk(
        IEnumerable<PartitionOwnership> currentPartitionOwnershipsForConsumerGroup)
    {
        return new ClaimPartitionOwnershipResponse(
            currentPartitionOwnershipsForConsumerGroup,
            ResultOneOfOk.Instance);
    }

    public sealed record ResultOneOfPartitionOwnershipObsolete
    {
        public static ResultOneOfPartitionOwnershipObsolete Instance { get; } = new ResultOneOfPartitionOwnershipObsolete();
    }

    public sealed record ResultOneOfPermissionDenied(IReadOnlyCollection<string> ProhibitedTopics);

    public sealed record ResultOneOfOk
    {
        public static ResultOneOfOk Instance { get; } = new ResultOneOfOk();
    };
}