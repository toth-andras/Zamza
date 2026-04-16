using Zamza.Consumer;

namespace ConsumerTest;

public sealed class TestMessageProcessor : IMessageCustomProcessor<string, string>
{
    public Task<ProcessResult> Process(
        ZamzaMessage<string, string> message,
        CancellationToken cancellationToken)
    {
        Console.WriteLine($"Consumed message: {message.Value}");
        
        return Task.FromResult(ProcessResult.RetryableFail);
    }
}