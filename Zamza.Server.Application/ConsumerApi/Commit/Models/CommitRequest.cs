using Zamza.Server.Models.ConsumerApi.Commit;

namespace Zamza.Server.Application.ConsumerApi.Commit.Models;

public sealed record CommitRequest(
    string ConsumerGroup,
    IReadOnlyCollection<RetryableMessage> RetryableMessages,
    IReadOnlyCollection<FailedMessage> FailedMessages);