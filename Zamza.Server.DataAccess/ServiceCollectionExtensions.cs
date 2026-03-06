using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.DataAccess.Common.Connections;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;

namespace Zamza.Server.DataAccess;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataAccess(this IServiceCollection services)
    {
        services.AddFluentMigrator();
        services.AddConnectionFactory();

        services.AddTransient<IPartitionOwnershipRepository, PartitionOwnershipRepository>();
        services.AddTransient<IRetryQueueRepository, RetryQueueRepository>();
        
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

    private static IServiceCollection AddConnectionFactory(this IServiceCollection services)
    {
        const string dbConnectionStringEnvironmentVariableName = "DB_CONNECTION_STRING";

        var connectionString = Environment.GetEnvironmentVariable(dbConnectionStringEnvironmentVariableName)
            ?? throw new ApplicationException($"{dbConnectionStringEnvironmentVariableName} must be set");
        
        services.AddNpgsqlDataSource(connectionString);
        services.AddSingleton<IConnectionFactory, ConnectionFactory>();

        return services;
    }
}