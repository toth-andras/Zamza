using Zamza.Server.Models.Validators;

namespace Zamza.Server.Application.UserApi.DLQ.Models;

public sealed record DLQMessageToDelete
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }

    public DLQMessageToDelete(
        string topic,
        int partition,
        long offset)
    {
        ThrowBadRequest.IfEmpty(topic, "Message Topic");

        Topic = topic;
        Partition = partition;
        Offset = offset;
    }
}