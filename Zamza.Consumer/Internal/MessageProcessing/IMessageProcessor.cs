using Zamza.Consumer.Internal.MessageProcessing.Models;

namespace Zamza.Consumer.Internal.MessageProcessing;

internal interface IMessageProcessor<TKey, TValue>
{
    Task<MessageSetProcessingResult<TKey, TValue>> ProcessMessages(
        ZamzaConsumerSettings<TKey, TValue> config,
        IReadOnlyCollection<ZamzaMessage<TKey, TValue>> messages,
        CancellationToken cancellationToken);
}