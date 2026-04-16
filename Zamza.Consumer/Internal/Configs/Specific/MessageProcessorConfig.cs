namespace Zamza.Consumer.Internal.Configs.Specific;

internal sealed record MessageProcessorConfig<TKey, TValue>
{
    public int MaxRetriesCount { get; }
    public TimeSpan MinRetriesGap { get; }
    public TimeSpan? ProcessingPeriod { get; }
    public Func<ZamzaMessage<TKey, TValue>, TimeSpan>? RetryGapEvaluator { get; }

    public MessageProcessorConfig(
        int maxRetriesCount,
        TimeSpan minRetriesGap,
        TimeSpan? processingPeriod,
        Func<ZamzaMessage<TKey, TValue>, TimeSpan>? retryGapEvaluator)
    {
        ValidateMaxRetriesCount(maxRetriesCount);
        ValidateMinRetriesGap(minRetriesGap);
        ValidateProcessingPeriod(processingPeriod);
        
        MaxRetriesCount = maxRetriesCount;
        MinRetriesGap = minRetriesGap;
        ProcessingPeriod = processingPeriod;
        RetryGapEvaluator = retryGapEvaluator;
    }

    public static MessageProcessorConfig<TKey, TValue> Default => new(
        maxRetriesCount: 5,
        minRetriesGap: TimeSpan.FromSeconds(1),
        processingPeriod: null,
        retryGapEvaluator: null);

    private static void ValidateMaxRetriesCount(int number)
    {
        if (number >= 0)
        {
            return;
        }
        
        throw new ArgumentOutOfRangeException(
            message: "The maximum number of retries must be greater than zero.",
            innerException: null);
    }
    private static void ValidateMinRetriesGap(TimeSpan timeSpan)
    {
        if (timeSpan >= TimeSpan.Zero)
        {
            return;
        }
        
        throw new ArgumentOutOfRangeException(
            message: "The gap between retries must be a non-negative Timespan",
            innerException: null);
    }
    private static void ValidateProcessingPeriod(TimeSpan? timeSpan)
    {
        if (timeSpan is null || timeSpan.Value >= TimeSpan.Zero)
        {
            return;
        }
        
        throw new ArgumentOutOfRangeException(
            message: "The processing period (if set) must be a non-negative TimeSpan.",
            innerException: null);
    }
}