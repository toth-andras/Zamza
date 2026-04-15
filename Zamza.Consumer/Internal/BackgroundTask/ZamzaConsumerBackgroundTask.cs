using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Zamza.Consumer.Internal.ConsumptionController;

namespace Zamza.Consumer.Internal.BackgroundTask;

internal sealed class ZamzaConsumerBackgroundTask<TKey, TValue> : BackgroundService
{
    private readonly IConsumptionController<TKey, TValue> _consumptionController;
    private readonly ILogger<ZamzaConsumerBackgroundTask<TKey, TValue>> _logger;

    public ZamzaConsumerBackgroundTask(
        IConsumptionController<TKey, TValue> consumptionController,
        ILogger<ZamzaConsumerBackgroundTask<TKey, TValue>> logger)
    {
        _consumptionController = consumptionController;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting consumption controller");

        try
        {
            await _consumptionController.RunMainLoop(stoppingToken);
        }
        catch
        {
            // pass
        }
    }
}