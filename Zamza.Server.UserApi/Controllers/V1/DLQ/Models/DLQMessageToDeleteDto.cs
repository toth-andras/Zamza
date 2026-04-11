using System.ComponentModel.DataAnnotations;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

public sealed class DLQMessageToDeleteDto
{
    /// <summary>
    /// The topic of the message.
    /// </summary>
    [Required]
    public required string Topic { get; init; }
    
    /// <summary>
    /// The partition of the message.
    /// </summary>
    [Required]
    public required int Partition { get; init; }
    
    /// <summary>
    /// The offset of the message.
    /// </summary>
    [Required]
    public required long Offset { get; init; }
}