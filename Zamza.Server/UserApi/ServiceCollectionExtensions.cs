namespace Zamza.Server.UserApi;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddUserApiLayer(this IServiceCollection services)
    {
        services.AddEndpointsApiExplorer();
        services.AddOpenApi();
        services.AddControllers();
        
        return services;
    }
}