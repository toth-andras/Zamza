using Zamza.Server.Models.Validators;

namespace Zamza.Server.Application.UserApi.DLQ.Models;

public sealed record DeleteDLQMessagesRequest
{
    public string ConsumerGroup { get; init; }
    public IReadOnlyCollection<DLQMessageToDelete> Messages { get; }

    public DeleteDLQMessagesRequest(
        string consumerGroup,
        IReadOnlyCollection<DLQMessageToDelete> messages)
    {
        ThrowBadRequest.IfEmpty(consumerGroup, "Consumer group");
        
        ConsumerGroup = consumerGroup;
        Messages = messages;
    }
}