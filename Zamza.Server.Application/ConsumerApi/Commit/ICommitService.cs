using Zamza.Server.Application.ConsumerApi.Commit.Models;

namespace Zamza.Server.Application.ConsumerApi.Commit;

public interface ICommitService
{
    Task<CommitResponse> Commit(
        CommitRequest request,
        CancellationToken cancellationToken);
}