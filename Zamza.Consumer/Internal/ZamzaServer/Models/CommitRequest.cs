using Zamza.Consumer.Internal.Models;

namespace Zamza.Consumer.Internal.ZamzaServer.Models;

internal sealed record CommitRequest<TKey, TValue>(
    string ConsumerId,
    string ConsumerGroup,
    IReadOnlyCollection<PartitionOwnership> PartitionsToCommit,
    IReadOnlyCollection<ZamzaMessage<TKey, TValue>> ProcessedMessages,
    IReadOnlyCollection<(ZamzaMessage<TKey, TValue> Message, TimeSpan NextRetryAfter)> MessagesWithRetryableFailure,
    IReadOnlyCollection<(ZamzaMessage<TKey, TValue> Message, DateTime FailedAtUtc)> MessagesWithCompleteFailure);