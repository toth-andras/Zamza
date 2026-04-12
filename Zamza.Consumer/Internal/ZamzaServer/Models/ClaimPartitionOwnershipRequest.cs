using Zamza.Consumer.Internal.Models;

namespace Zamza.Consumer.Internal.ZamzaServer.Models;

internal sealed record ClaimPartitionOwnershipRequest (
    string ConsumerId,
    string ConsumerGroup,
    IReadOnlyCollection<PartitionOwnership> ClaimedPartitions);