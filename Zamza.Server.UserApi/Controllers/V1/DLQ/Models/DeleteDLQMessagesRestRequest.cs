using System.ComponentModel.DataAnnotations;

namespace Zamza.Server.UserApi.Controllers.V1.DLQ.Models;

public sealed class DeleteDLQMessagesRestRequest
{
    /// <summary>
    /// The consumer group the messages were saved for.
    /// </summary>
    [Required]
    public required string ConsumerGroup { get; init; }
    
    /// <summary>
    /// The messages to be deleted.
    /// </summary>
    [Required]
    public required DLQMessageToDeleteDto[] Messages { get; init; }
}