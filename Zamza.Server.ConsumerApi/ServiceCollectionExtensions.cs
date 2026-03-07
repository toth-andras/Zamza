using Microsoft.Extensions.DependencyInjection;

namespace Zamza.Server.ConsumerApi;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddConsumerApiLayer(this IServiceCollection services)
    {
        services.AddGrpc();
        
        return services;
    }
}