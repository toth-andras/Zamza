using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository;

namespace Zamza.Server.Application.Observability.BackgroundTasks;

internal abstract class BackgroundTaskWithLeadership : BackgroundService
{
    private readonly IInstanceLeadershipRepository _leadershipRepository;
    private readonly ILogger<BackgroundTaskWithLeadership> _logger;
    
    protected abstract string BackgroundTaskName { get; }
    protected abstract Guid InstanceId { get; }
    protected abstract TimeSpan CycleTime { get; }

    protected BackgroundTaskWithLeadership(
        IInstanceLeadershipRepository leadershipRepository,
        ILogger<BackgroundTaskWithLeadership> logger)
    {
        _leadershipRepository = leadershipRepository;
        _logger = logger;
    }

    protected abstract Task ExecuteCycle(CancellationToken cancellationToken);

    protected sealed override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteAsyncInner(cancellationToken);
        }
        catch (Exception exception)
        {
            _logger.LogCritical(
                exception,
                "Background task \'{TaskName}\' stopped due to unexpected error",
                BackgroundTaskName);
        }
    }
    private async Task ExecuteAsyncInner(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(CycleTime, cancellationToken);

            var isLeader = await _leadershipRepository.TryBecomeLeader(
                BackgroundTaskName,
                InstanceId,
                CycleTime.Add(TimeSpan.FromSeconds(3)), // To prioritize the current leader
                cancellationToken);

            if (isLeader is false)
            {
                _logger.LogDebug(
                    "Current instance is not leader for \'{TaskName}\' background task for the current cycle",
                    BackgroundTaskName);
                continue;
            }
            
            _logger.LogDebug(
                "Got leadership for \'{TaskName}\' background task for the current cycle",
                BackgroundTaskName);

            try
            {
                await ExecuteCycle(cancellationToken);
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "An exception occurred during \'{TaskName}\' background task execution",
                    BackgroundTaskName);
            }
        }
    }
}