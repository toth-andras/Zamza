using OneOf;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Commit.Models;

public sealed record CommitResponse(
    IReadOnlyCollection<PartitionOwnership> CurrentOwners,
    IReadOnlyCollection<TopicPartitionOffset> MessagesOfUnownedPartitions,
    IReadOnlyCollection<TopicPartitionOffset> MessagesOfProhibitedTopics);