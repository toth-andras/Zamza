namespace Zamza.Consumer;

/// <summary>
/// Represents the result of processing of the <see cref="ZamzaMessage{TKey,TValue}"/>.
/// </summary>
public enum ProcessResult
{
    /// <summary>
    /// The message was processed successfully.
    /// </summary>
    Success,
    
    /// <summary>
    /// An error occured, the processing must be retried. 
    /// </summary>
    RetryableFail,
    
    /// <summary>
    /// An error occured, the message must be saved into the DLQ.
    /// </summary>
    CompleteFail
}