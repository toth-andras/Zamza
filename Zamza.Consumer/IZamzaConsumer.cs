namespace Zamza.Consumer;

public interface IZamzaConsumer
{
    /// <summary>
    /// Returns a task that represents the lifetime of consumption being performed.
    /// </summary>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    Task ExecuteAsync(CancellationToken stoppingToken);

    /// <summary>
    /// Stop the consumer.
    /// </summary>
    void Stop();
}