using Zamza.Consumer.Internal.Models;

namespace Zamza.Consumer.Internal.ZamzaServer.Models;

internal sealed record ClaimPartitionOwnershipResult(
    bool IsSuccessful,
    IReadOnlyCollection<PartitionOwnership> ConsumerGroupPartitionOwnership);