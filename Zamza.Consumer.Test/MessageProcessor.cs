namespace Zamza.Consumer.Test;

public class MessageProcessor : IMessageCustomProcessor<string, string>
{
    private int counter = 0;
    public Task<ProcessResult> Process(
        ZamzaMessage<string, string> message,
        CancellationToken cancellationToken)
    {
        counter++;
        Console.WriteLine(
            $"Consumed message: {message.Value} Offset: {message.Offset} FromKafka: {message.IsFromKafka}");

        var result = counter switch
        {
            4 => Task.FromResult(ProcessResult.RetryableFail),
            5 => Task.FromResult(ProcessResult.CompleteFail),

            _ => Task.FromResult(ProcessResult.Success)
        };

        if (counter == 5)
        {
            counter = 0;
        }
        return result;
    }
}