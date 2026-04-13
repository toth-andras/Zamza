using Zamza.Consumer.Internal.Models;

namespace Zamza.Consumer.Internal.ZamzaServer.Models;

internal sealed record CommitResult(
    IReadOnlyCollection<PartitionOwnership> ConsumerGroupPartitionOwnerships,
    IReadOnlyCollection<CommitResult.PartitionWithIrrelevantOwnership> PartitionsWithIrrelevantOwnership)
{
    public sealed record PartitionWithIrrelevantOwnership(
        string Topic,
        int Partition);
}