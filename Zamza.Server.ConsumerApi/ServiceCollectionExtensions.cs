using Microsoft.Extensions.DependencyInjection;

namespace Zamza.Server.ConsumerApi;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddConsumerApi(this IServiceCollection services)
    {
        return services;
    }
}