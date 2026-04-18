namespace Zamza.Consumer.Test;

public class MessageProcessor : IMessageCustomProcessor<string, string>
{
    public Task<ProcessResult> Process(
        ZamzaMessage<string, string> message,
        CancellationToken cancellationToken)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        var source = message.IsFromKafka ? "Kafka" : "Zamza.Server";
        
        Console.WriteLine(
            $"[{timestamp}] Consumed message: {message.Value}. Source: {source}. Retries count: {message.RetriesCount}");

        return Task.FromResult(ProcessResult.RetryableFail);
    }
}