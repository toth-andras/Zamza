using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;

namespace Zamza.Server.DataAccess;

public static class ServiceProviderExtensions
{
    public static IServiceProvider RunMigrations(this IServiceProvider serviceProvider)
    {
        using var scope = serviceProvider.CreateScope();
        var migrator = scope.ServiceProvider.GetRequiredService<IMigrationRunner>();
        migrator.MigrateUp();
        
        return serviceProvider;
    }
}