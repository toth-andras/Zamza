namespace Zamza.Consumer.Internal.MessageProcessing.Models;

internal sealed record MessageSetProcessingResult<TKey, TValue>(
    IReadOnlyCollection<ZamzaMessage<TKey, TValue>> ProcessedMessages,
    IReadOnlyCollection<(ZamzaMessage<TKey, TValue> Message, TimeSpan NextRetryAfter)> MessagesWithRetryableFailure,
    IReadOnlyCollection<(ZamzaMessage<TKey, TValue> Message, DateTime FailedAtUtc)> MessagesWithCompleteFailure);