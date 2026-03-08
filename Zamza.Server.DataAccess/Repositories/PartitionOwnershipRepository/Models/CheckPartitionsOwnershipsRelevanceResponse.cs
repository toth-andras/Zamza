using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.Models;

public sealed record CheckPartitionsOwnershipsRelevanceResponse(
    bool IsOwnershipRelevant,
    IReadOnlyCollection<PartitionOwnership> CurrentOwnerships);