using Zamza.Consumer.Internal.Models;

namespace Zamza.Consumer.Internal.ZamzaServer.Models;

internal sealed record FetchResult<TKey, TValue>
{
    private readonly IReadOnlyCollection<ZamzaMessage<TKey, TValue>> _messages;
    
    public IReadOnlyCollection<PartitionOwnership> ConsumerGroupPartitionOwnerships { get; }

    public IReadOnlyCollection<ZamzaMessage<TKey, TValue>> Messages
    {
        get
        {
            if (IsFetchSuccessful is false)
            {
                throw new InvalidOperationException("The fetch was not successful, no messages to get");
            }
            
            return _messages;
        }
        
        private init => _messages = value;
    }

    public bool IsFetchSuccessful { get; }
    
    private FetchResult(
        bool isFetchSuccessful,
        IReadOnlyCollection<PartitionOwnership> consumerGroupPartitionOwnerships,
        IReadOnlyCollection<ZamzaMessage<TKey, TValue>> messages)
    {
        IsFetchSuccessful = isFetchSuccessful;
        ConsumerGroupPartitionOwnerships = consumerGroupPartitionOwnerships;
        Messages = messages;
    }

    public static FetchResult<TKey, TValue> AsOk(
        IReadOnlyCollection<PartitionOwnership> consumerGroupPartitionOwnerships,
        IReadOnlyCollection<ZamzaMessage<TKey, TValue>> messages)
    {
        return new FetchResult<TKey, TValue>(
            isFetchSuccessful: true,
            consumerGroupPartitionOwnerships: consumerGroupPartitionOwnerships,
            messages: messages);
    }

    public static FetchResult<TKey, TValue> AsPartitionOwnershipObsolete(
        IReadOnlyCollection<PartitionOwnership> consumerGroupPartitionOwnerships)
    {
        return new FetchResult<TKey, TValue>(
            isFetchSuccessful: false,
            consumerGroupPartitionOwnerships: consumerGroupPartitionOwnerships,
            messages: null!);
    }
}