using Zamza.Server.Application.ConsumerApi.Commit.Models;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.Application.ConsumerApi.Commit;

public interface ICommitService
{
    Task<CommitResponse> Commit(
        CommitRequest request,
        CancellationToken cancellationToken);
}