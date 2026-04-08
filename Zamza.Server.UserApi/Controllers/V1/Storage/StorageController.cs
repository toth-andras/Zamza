using Microsoft.AspNetCore.Mvc;
using Zamza.Server.Application.UserApi.Storage;
using Zamza.Server.UserApi.Controllers.V1.Storage.Models;

namespace Zamza.Server.UserApi.Controllers.V1.Storage;

[ApiController]
[Route("Storage")]
public sealed class StorageController : ControllerBase
{
    private readonly IStorageService _storageService;

    public StorageController(IStorageService storageService)
    {
        _storageService = storageService;
    }

    /// <summary>
    /// Removes the message both from the retry queue and the DLQ.
    /// </summary>
    [HttpDelete("delete-message")]
    public async Task<IActionResult> DeleteMessage(
        [FromBody] DeleteMessageRequest request,
        CancellationToken cancellationToken)
    {
        await _storageService.DeleteMessage(
            new Application.UserApi.Storage.Models.DeleteMessageRequest(
                request.Topic,
                request.Partition,
                request.Offset),
            cancellationToken);
        
        return Ok();
    }
}