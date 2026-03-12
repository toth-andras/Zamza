using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.Application.ConsumerApi.Commit;
using Zamza.Server.Application.ConsumerApi.Fetch;
using Zamza.Server.Application.ConsumerApi.Leave;
using Zamza.Server.Application.ConsumerApi.Ping;

namespace Zamza.Server.Application;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services)
    {
        services.AddConsumerApi();
        
        return services;
    }

    private static IServiceCollection AddConsumerApi(this IServiceCollection services)
    {
        services.AddTransient<IFetchService, FetchService>();
        services.AddTransient<IPingService, PingService>();
        services.AddTransient<ICommitService, CommitService>();
        services.AddTransient<ILeaveService, LeaveService>();
        
        return services;
    }
}