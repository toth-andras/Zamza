using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;

namespace Zamza.Server.DataAccess;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataAccess(this IServiceCollection services)
    {
        services.AddFluentMigrator();
        
        return services;
    }

    private static IServiceCollection AddFluentMigrator(this IServiceCollection services)
    {
        services
            .AddFluentMigratorCore()
            .ConfigureRunner(runnerBuilder => runnerBuilder
                .AddPostgres()
                .WithGlobalConnectionString("Server=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres")
                .ScanIn(typeof(ServiceCollectionExtensions).Assembly).For.Migrations())
            .BuildServiceProvider(false);
        
        return services;
    }
}