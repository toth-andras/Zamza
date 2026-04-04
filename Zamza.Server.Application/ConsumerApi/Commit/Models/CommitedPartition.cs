using Zamza.Server.Models.Validators;

namespace Zamza.Server.Application.ConsumerApi.Commit.Models;

public sealed record CommitedPartition
{
    public string Topic { get; }
    public int Partition { get; }
    public long OwnershipEpoch { get; }

    public CommitedPartition(
        string topic,
        int partition,
        long ownershipEpoch)
    {
        ThrowBadRequest.IfEmpty(topic, "Commited partition Topic");
        ThrowBadRequest.IfNegative(ownershipEpoch, "Commited partition Ownership epoch");
        
        Topic = topic;
        Partition = partition;
        OwnershipEpoch = ownershipEpoch;
    }
}