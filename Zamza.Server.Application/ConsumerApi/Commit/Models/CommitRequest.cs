using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Commit.Models;

public sealed record CommitRequest (
    string ConsumerId,
    string ConsumerGroup,
    MessageKeysSet ProcessedMessages,
    IReadOnlyList<FailedMessage> FailedMessages,
    IReadOnlyList<ConsumerApiMessage> PoisonedMessages,
    ConsumerPartitionOwnershipsSet ConsumerPartitionOwnerships);