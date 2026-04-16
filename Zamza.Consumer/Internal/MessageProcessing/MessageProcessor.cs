using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.Configs;
using Zamza.Consumer.Internal.MessageProcessing.Models;
using Zamza.Consumer.Internal.Utils.DateTimeProvider;

namespace Zamza.Consumer.Internal.MessageProcessing;

internal sealed class MessageProcessor<TKey, TValue> : IMessageProcessor<TKey, TValue>
{
    private readonly IMessageCustomProcessor<TKey, TValue> _customProcessor;
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly ILogger<MessageProcessor<TKey, TValue>> _logger;

    public MessageProcessor(
        IMessageCustomProcessor<TKey, TValue> customProcessor,
        IDateTimeProvider dateTimeProvider,
        ILogger<MessageProcessor<TKey, TValue>> logger)
    {
        _customProcessor = customProcessor;
        _dateTimeProvider = dateTimeProvider;
        _logger = logger;
    }

    public async Task<MessageSetProcessingResult<TKey, TValue>> ProcessMessages(
        ZamzaConsumerConfig<TKey, TValue> config,
        IReadOnlyCollection<ZamzaMessage<TKey, TValue>> messages,
        CancellationToken cancellationToken)
    {
        List<ZamzaMessage<TKey, TValue>> processedMessages = new(messages.Count);
        List<(ZamzaMessage<TKey, TValue> Message, TimeSpan NextRetryAfter)> messagesWithRetryableFailure = [];
        List<(ZamzaMessage<TKey, TValue> Message, DateTime FailedAtUtc)> messagesWithCompleteFailure = [];

        using var scope = _logger.BeginScope(
            "[MessageProcessing] ConsumerGroup: {ConsumerGroup}, ConsumerId: {ConsumerId}",
            config.MainInfo.ConsumerGroup,
            config.MainInfo.ConsumerId);
        
        foreach (var message in messages)
        {
            // Preprocessing
            if (IsFailedCompletely(message, _dateTimeProvider.UtcNow))
            {
                _logger.LogInformation(
                    "Message (Topic: \'{Topic}\', Partition: {Partition}, Offset: {Offset}) automatically set to completely failed",
                    message.Topic,
                    message.Partition,
                    message.Offset);
                
                messagesWithCompleteFailure.Add((message, _dateTimeProvider.UtcNow));
                continue;
            }

            // Custom processing
            ProcessResult processingResult;
            try
            {
                processingResult = await _customProcessor.Process(message, cancellationToken);
            }
            catch
            {
                processingResult = ProcessResult.CompleteFail;
            }
            
            // Postprocessing
            if (message.IsFromKafka is false)
            {
                // If the message is not from Kafka, then this is a retry
                message.IncrementRetriesCount();
            }
            
            if (processingResult == ProcessResult.RetryableFail &&
                message.RetriesCount >= message.MaxRetriesCount)
            {
                processingResult = ProcessResult.CompleteFail;
            }
            
            switch (processingResult)
            {
                case ProcessResult.Success:
                    processedMessages.Add(message);
                    break;
                
                case ProcessResult.RetryableFail:
                    messagesWithRetryableFailure.Add((message, GetGapBeforeNextRetry(message, config)));
                    break;
                
                case ProcessResult.CompleteFail:
                    messagesWithCompleteFailure.Add((message, _dateTimeProvider.UtcNow));
                    break;
                
                default:
                    throw new NotImplementedException($"Not supported message processing result: {processingResult}");
            }
            
            LogProcessingResult(message, processingResult);
        }

        return new MessageSetProcessingResult<TKey, TValue>(
            processedMessages, 
            messagesWithRetryableFailure, 
            messagesWithCompleteFailure);
    }

    private static bool IsFailedCompletely(ZamzaMessage<TKey, TValue> message, DateTime utcNow)
    {
        return 
            message.RetriesCount >= message.MaxRetriesCount ||
            (message.ProcessingDeadline is not null && message.ProcessingDeadline.Value <= utcNow);
    }

    private TimeSpan GetGapBeforeNextRetry(
        ZamzaMessage<TKey, TValue> message,
        ZamzaConsumerConfig<TKey, TValue> config)
    {
        if (config.MessageProcessor.RetryGapEvaluator is null)
        {
            return config.MessageProcessor.MinRetriesGap;
        }
        
        try
        {
            return config.MessageProcessor.RetryGapEvaluator.Invoke(message);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception, 
                $"Evaluation of the gap before next retry caused exception. " +
                $"Using {nameof(ZamzaConsumerConfig<,>.MessageProcessor.MinRetriesGap)}");
            return config.MessageProcessor.MinRetriesGap;
        }
    }

    private void LogProcessingResult(ZamzaMessage<TKey, TValue> message, ProcessResult processResult)
    {
        if (_logger.IsEnabled(LogLevel.Debug) is false)
        {
            return;
        }
        
        _logger.LogDebug(
            "Message (Topic: \'{Topic}\', Partition: {Partition}, Offset: {Offset}) processed with result: {Result}",
            message.Topic,
            message.Partition,
            message.Offset,
            processResult);
    }
}