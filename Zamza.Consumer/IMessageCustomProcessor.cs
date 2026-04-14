namespace Zamza.Consumer;

/// <summary>
/// Represents a common interface for a class
/// encapsulating custom logic of message processing.
/// </summary>
/// <typeparam name="TKey">Type of the message key.</typeparam>
/// <typeparam name="TValue">Type of the message value.</typeparam>
public interface IMessageCustomProcessor<TKey, TValue>
{
    Task<ProcessResult> Process(
        ZamzaMessage<TKey, TValue> message, 
        CancellationToken cancellationToken);
}