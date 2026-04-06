using Zamza.Server.Models.Validators;

namespace Zamza.Server.Application.UserApi.Storage.Models;

public sealed record DeleteMessageRequest
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }

    public DeleteMessageRequest(
        string topic,
        int partition,
        long offset)
    {
        ThrowBadRequest.IfEmpty(topic, "Delete message request Topic");
        
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }
}