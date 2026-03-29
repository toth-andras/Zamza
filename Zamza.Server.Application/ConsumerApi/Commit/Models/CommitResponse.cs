using Zamza.Server.Models.ConsumerApi.Common;

namespace Zamza.Server.Application.ConsumerApi.Commit.Models;

public sealed record CommitResponse(
    ConsumerGroupPartitionOwnershipSet ConsumerGroupPartitionOwnerships);