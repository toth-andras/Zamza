using Prometheus;

namespace Zamza.Server;

internal static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMetricsInfrastructure(this IServiceCollection services)
    {
        services.AddMetricServer(options => options.Port = 9090);
        
        return services;
    }
}