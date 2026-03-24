using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.ConsumerApi.Utils.DateTimeProvider;

namespace Zamza.Server.ConsumerApi;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddConsumerApiLayer(this IServiceCollection services)
    {
        services.AddGrpc();
        services.AddSingleton<IDateTimeProvider, DateTimeProvider>();
        
        return services;
    }
}