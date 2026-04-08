using System.ComponentModel.DataAnnotations;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

public sealed class GetDLQMessagesRestResponse
{
    /// <summary>
    /// The obtained messages.
    /// </summary>
    [Required]
    public required DLQMessageDto[] Messages { get; init; }
    
    /// <summary>
    /// Technical value for the pagination. Must be used
    /// in the next request.
    /// </summary>
    [Required]
    public required long Cursor { get; init; }
}