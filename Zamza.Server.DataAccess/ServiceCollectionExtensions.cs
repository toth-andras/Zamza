using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.DataAccess.Common.ConnectionsManagement;
using Zamza.Server.DataAccess.Common.DapperConfiguration;
using Zamza.Server.DataAccess.Repositories.ConsumerHeartbeatRepository;
using Zamza.Server.DataAccess.Repositories.DLQRepository;
using Zamza.Server.DataAccess.Repositories.InstanceLeadershipRepository;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;
using Zamza.Server.DataAccess.Repositories.RetryQueueRepository;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.DataAccess;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataAccessLayer(this IServiceCollection services)
    {
        var connectionString = GetConnectionString();
        
        services.AddFluentMigrator(connectionString);
        services.AddConnectionFactory(connectionString);
        
        DapperConfigurations.Configure();

        services.AddScoped<IPartitionOwnershipRepository, PartitionOwnershipRepository>();
        services.AddScoped<IRetryQueueRepository, RetryQueueRepository>();
        services.AddScoped<IDLQRepository, DLQRepository>();
        services.AddScoped<IConsumerHeartbeatRepository, ConsumerHeartbeatRepository>();
        services.AddTransient<IInstanceLeadershipRepository, InstanceLeadershipRepository>();
        
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
                               ?? throw new InternalException($"{dbConnectionStringEnvironmentVariableName} must be set");
    }
}