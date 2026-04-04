using Zamza.Server.Models.ConsumerApi.Commit;

namespace Zamza.Server.Application.ConsumerApi.Commit.Models;

public sealed record CommitRequest(
    string ConsumerId,
    string ConsumerGroup,
    IReadOnlyCollection<CommitedPartition> Partitions,
    IReadOnlyCollection<ProcessedMessage> ProcessedMessages,
    IReadOnlyCollection<RetryableMessage> RetryableMessages,
    IReadOnlyCollection<FailedMessage> FailedMessages);