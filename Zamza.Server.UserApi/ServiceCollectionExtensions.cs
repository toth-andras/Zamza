using Microsoft.Extensions.DependencyInjection;

namespace Zamza.Server.UserApi;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddUserApiLayer(this IServiceCollection services)
    {
        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen();
        services.AddControllers();
        
        return services;
    }
}