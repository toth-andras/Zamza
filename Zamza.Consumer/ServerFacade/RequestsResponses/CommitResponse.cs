using Zamza.Consumer.Models;

namespace Zamza.Consumer.ServerFacade.RequestsResponses;

internal sealed record CommitResponse(
    IReadOnlyCollection<PartitionOwnership> PartitionOwnershipsForConsumerGroup,
    HashSet<(string Topic, int Partition, long Offset)> UnownedPartitionsMessages,
    HashSet<(string Topic, int Partition, long Offset)> ProhibitedTopicsMessages);