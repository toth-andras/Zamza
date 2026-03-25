using Zamza.Server.Models.ConsumerApi.Common;
using Zamza.Server.Models.ConsumerApi.Fetch;

namespace Zamza.Server.Application.ConsumerApi.Fetch.Models;

public sealed record FetchResponse(
    FetchResult Result,
    ConsumerGroupPartitionOwnershipSet ConsumerGroupPartitionOwnerships,
    IReadOnlyCollection<FetchedMessage>? FetchedMessages)
{
    public static FetchResponse AsOk(
        ConsumerGroupPartitionOwnershipSet consumerGroupPartitionOwnerships,
        IReadOnlyCollection<FetchedMessage> messages)
    {
        return new FetchResponse(
            FetchResult.Ok,
            consumerGroupPartitionOwnerships,
            messages);
    }

    public static FetchResponse AsObsoleteOwnership(ConsumerGroupPartitionOwnershipSet consumerGroupPartitionOwnerships)
    {
        return new FetchResponse(
            FetchResult.ObsoleteOwnership,
            consumerGroupPartitionOwnerships,
            FetchedMessages: null);
    }
}