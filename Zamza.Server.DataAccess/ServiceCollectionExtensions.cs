using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.DataAccess.Common.Connections;
using Zamza.Server.DataAccess.Common.DapperMapping;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

namespace Zamza.Server.DataAccess;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataAccess(this IServiceCollection services)
    {
        var connectionString = GetConnectionString();
        
        services.AddFluentMigrator(connectionString);
        services.AddConnectionFactory(connectionString);
        
        DapperMappingExtensions.Configure();

        services.AddTransient<IPartitionOwnershipRepository, PartitionOwnershipRepository>();
        services.AddTransient<IRetryQueueRepository, RetryQueueRepository>();
        
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
        services.AddSingleton<IConnectionFactory, ConnectionFactory>();

        return services;
    }

    private static string GetConnectionString()
    {
        const string dbConnectionStringEnvironmentVariableName = "DB_CONNECTION_STRING";
        return Environment.GetEnvironmentVariable(dbConnectionStringEnvironmentVariableName)
                               ?? throw new ApplicationException($"{dbConnectionStringEnvironmentVariableName} must be set");
    }
}