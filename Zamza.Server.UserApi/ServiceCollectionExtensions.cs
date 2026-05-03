using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.UserApi.Middlewares;

namespace Zamza.Server.UserApi;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddUserApiLayer(this IServiceCollection services)
    {
        services.AddMiddlewares();
        
        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen(options =>
        {
            var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
            var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
            options.IncludeXmlComments(xmlPath);
        });
        services.AddControllers();
        
        return services;
    }

    private static IServiceCollection AddMiddlewares(this IServiceCollection services)
    {
        services.AddTransient<ErrorHandlingMiddleware>();
        
        return services;
    }
}