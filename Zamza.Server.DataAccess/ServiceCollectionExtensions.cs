using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.DapperMapping;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Utils.DateTimeProvider;

namespace Zamza.Server.DataAccess;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataAccessLayer(this IServiceCollection services)
    {
        var connectionString = GetConnectionString();
        
        services.AddFluentMigrator(connectionString);
        services.AddConnectionFactory(connectionString);

        services.AddSingleton<IDateTimeProvider, DateTimeProvider>();
        
        DapperMappingExtensions.Configure();

        services.AddTransient<IPartitionOwnershipRepository, PartitionOwnershipRepository>();
        
        return services;
    }

    private static IServiceCollection AddFluentMigrator(this IServiceCollection services, string connectionString)
    {
        services
            .AddFluentMigratorCore()
            .ConfigureRunner(runnerBuilder => runnerBuilder
                .AddPostgres()
                .WithGlobalConnectionString(connectionString)
                .ScanIn(typeof(ServiceCollectionExtensions).Assembly).For.Migrations())
            .BuildServiceProvider(false);
        
        return services;
    }

    private static IServiceCollection AddConnectionFactory(this IServiceCollection services, string connectionString)
    {
        services.AddNpgsqlDataSource(connectionString);
        services.AddSingleton<IDbConnectionsManager, DbConnectionsManager>();

        return services;
    }

    private static string GetConnectionString()
    {
        const string dbConnectionStringEnvironmentVariableName = "DB_CONNECTION_STRING";
        return Environment.GetEnvironmentVariable(dbConnectionStringEnvironmentVariableName)
                               ?? throw new ApplicationException($"{dbConnectionStringEnvironmentVariableName} must be set");
    }
}