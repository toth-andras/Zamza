using Zamza.Server.Application.ConsumerApi.Commit.Models;

namespace Zamza.Server.Application.ConsumerApi.Commit;

internal sealed class CommitService : ICommitService
{
    public Task<CommitResponse> Commit(
        CommitRequest request,
        CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}