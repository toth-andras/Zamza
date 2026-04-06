using Zamza.Server.Application.UserApi.Storage.Models;

namespace Zamza.Server.Application.UserApi.Storage;

public interface IStorageService
{
    Task DeleteMessage(
        DeleteMessageRequest request,
        CancellationToken cancellationToken);
}