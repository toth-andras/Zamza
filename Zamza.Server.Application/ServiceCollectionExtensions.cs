using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.Application.ConsumerApi.Fetch;

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
        
        return services;
    }
}