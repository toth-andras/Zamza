using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.ConsumerApi.Interceptors;
using Zamza.Server.ConsumerApi.Utils.DateTimeProvider;

namespace Zamza.Server.ConsumerApi;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddConsumerApiLayer(this IServiceCollection services)
    {
        services.AddGrpc(options =>
        {
            options.AddInterceptors();
        });
        services.AddSingleton<IDateTimeProvider, DateTimeProvider>();
        
        return services;
    }

    private static GrpcServiceOptions AddInterceptors(this GrpcServiceOptions options)
    {
        options.Interceptors.Add<ExceptionInterceptor>();
        options.Interceptors.Add<MetricsInterceptor>();
        
        return options;
    }
}