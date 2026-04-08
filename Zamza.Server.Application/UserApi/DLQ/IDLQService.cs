using Zamza.Server.Application.UserApi.DLQ.Models;

namespace Zamza.Server.Application.UserApi.DLQ;

public interface IDLQService
{
    Task<GetDLQMessagesResponse> GetMessages(
        GetDLQMessagesRequest request,
        CancellationToken cancellationToken);
}