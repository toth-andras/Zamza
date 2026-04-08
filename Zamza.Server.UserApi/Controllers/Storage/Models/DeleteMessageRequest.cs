using System.ComponentModel.DataAnnotations;

namespace Zamza.Server.UserApi.Controllers.Storage.Models;

public sealed class DeleteMessageRequest
{
    /// <summary>
    /// The name of the original Kafka topic of the message.
    /// </summary>
    [Required]
    public required string Topic { get; init; }
    
    /// <summary>
    /// The original partition in Kafka topic.
    /// </summary>
    [Required]
    public required int Partition { get; init; }
    
    /// <summary>
    /// The original offset of the message in Kafka.
    /// </summary>
    [Required]
    public required long Offset { get; init; }
}