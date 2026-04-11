using Microsoft.AspNetCore.Mvc;
using Zamza.Server.Application.UserApi.DLQ;
using Zamza.Server.Application.UserApi.DLQ.Models;
using Zamza.Server.UserApi.Controllers.V1.DLQ.Mapping;
using Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ;

[ApiController]
[Route("DLQ/v1")]
public sealed class DLQController : ControllerBase
{
    private readonly IDLQService _dlqService;

    public DLQController(IDLQService dlqService)
    {
        _dlqService = dlqService;
    }

    /// <summary>
    /// Get the messages stored in Zazmza DLQ.
    /// </summary>
    /// <param name="limit">No more than this many messages will be returned.</param>
    /// <param name="cursor">
    /// Technical value for the pagination. To get the first page,
    /// set the value to null. To get the next page, set the value to
    /// the value returned in the previous request.
    /// </param>
    /// <param name="cancellationToken"></param>
    /// <returns>Messages stored in the DLQ.</returns>
    [HttpGet("messages")]
    public async Task<ActionResult<GetDLQMessagesRestResponse>> Get(
        [FromQuery] int limit,
        [FromQuery] long? cursor,
        CancellationToken cancellationToken)
    {
        var request = new GetDLQMessagesRequest(limit, cursor);

        var response = await _dlqService.GetMessages(request, cancellationToken);

        return Ok(response.ToRest());
    }

    /// <summary>
    /// Deletes the messages stored in DLQ by any member of given consumer group.
    /// </summary>
    [HttpDelete("delete-messages")]
    public async Task<IActionResult> Delete(
        [FromBody] DeleteDLQMessagesRestRequest request,
        CancellationToken cancellationToken)
    {
        var requestModel =  request.ToModel();
        
        await _dlqService.DeleteMessages(requestModel, cancellationToken);
        
        return Ok();
    }
}