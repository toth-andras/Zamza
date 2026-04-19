using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;
using Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository;

namespace Zamza.Server.Application.Observability.BackgroundTasks;

internal abstract class BackgroundTaskWithLeadership : BackgroundService
{
    private readonly IInstanceLeadershipRepository _leadershipRepository;
    private readonly ILogger<BackgroundTaskWithLeadership> _logger;
    
    private static readonly Gauge LeadershipGauge = Metrics.CreateGauge(
        "zamza_background_tasks_leadership",
        "Shows what instance is currently a leader for the background task",
        new GaugeConfiguration
        {
            LabelNames = ["task_name", "server_instance"]
        });
    
    protected abstract string BackgroundTaskName { get; }
    protected abstract TimeSpan CycleTime { get; }
    protected Guid ServerInstanceId { get; } = ObservabilityContstants.ServiceInstanceId;

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
        const int backgroundLeadershipIsNotLeaderValue = 0;
        const int backgroundLeadershipIsLeaderValue = 1;
        
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(CycleTime, cancellationToken);

            var isLeader = await _leadershipRepository.TryBecomeLeader(
                BackgroundTaskName,
                ServerInstanceId,
                CycleTime.Add(TimeSpan.FromSeconds(3)), // To prioritize the current leader
                cancellationToken);

            if (isLeader is false)
            {
                LeadershipGauge
                    .WithLabels([BackgroundTaskName, ServerInstanceId.ToString()])
                    .Set(val: backgroundLeadershipIsNotLeaderValue); 
                
                _logger.LogDebug(
                    "Current instance is not leader for \'{TaskName}\' background task for the current cycle",
                    BackgroundTaskName);
                continue;
            }
            
            LeadershipGauge
                .WithLabels([BackgroundTaskName, ServerInstanceId.ToString()])
                .Set(val: backgroundLeadershipIsLeaderValue); 
            
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