using Zamza.Consumer.Models;

namespace Zamza.Consumer.CustomProcessing;

public interface ICustomProcessor<TKey, TValue>
{
    Task<ProcessVerdict> Process(ZamzaMessage<TKey, TValue> message, CancellationToken cancellationToken);
}