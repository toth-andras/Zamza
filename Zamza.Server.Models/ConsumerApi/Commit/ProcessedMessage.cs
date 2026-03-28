using Zamza.Server.Models.Validators;

namespace Zamza.Server.Models.ConsumerApi.Commit;

public sealed record ProcessedMessage
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }

    public ProcessedMessage(string topic, int partition, long offset)
    {
        ThrowBadRequest.IfEmpty(topic, "Processed message Topic");
        
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }
}