using Zamza.Consumer.Internal.Configs.Specific;

namespace Zamza.Consumer.Options;

public sealed class MessageProcessingOptions<TKey, TValue>
{
    /// <summary>
    /// No more than this many reprocessing attempts will be performed for a message.
    /// </summary>
    public int MaxRetriesCount { get; init; } = MessageProcessorConfig<TKey, TValue>.Default.MaxRetriesCount;
    
    /// <summary>
    /// The minimal delay between two reprocessing attempts.
    /// </summary>
    public TimeSpan MinRetriesGap { get; init; } = MessageProcessorConfig<TKey, TValue>.Default.MinRetriesGap;
    
    /// <summary>
    /// Represents the period of time after the initial processing, in which
    /// the message must be successfully reprocessed, otherwise, it will be
    /// considered to be poisoned.
    /// </summary>
    public TimeSpan? ProcessingPeriod { get; init; } = MessageProcessorConfig<TKey, TValue>.Default.ProcessingPeriod;
    
    /// <summary>
    /// Evaluates the minimal delay before the next reprocessing attempt.
    /// </summary>
    /// <remarks>
    /// The evaluator is called after every reprocessing attempt before commiting results to Zamza.Server.
    /// </remarks>
    public Func<ZamzaMessage<TKey, TValue>, TimeSpan>? RetryGapEvaluator { get; init; } = MessageProcessorConfig<TKey, TValue>.Default.RetryGapEvaluator;
}