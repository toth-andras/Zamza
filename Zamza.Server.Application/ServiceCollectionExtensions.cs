using Microsoft.Extensions.DependencyInjection;
using Zamza.Server.Application.ConsumerApi.ClaimPartitionOwnership;
using Zamza.Server.Application.ConsumerApi.Commit;
using Zamza.Server.Application.ConsumerApi.Fetch;
using Zamza.Server.Application.ConsumerApi.Leave;

namespace Zamza.Server.Application;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services)
    {
        services.AddConsumerApi();
        
        return services;
    }

    private static IServiceCollection AddConsumerApi(this IServiceCollection services)
    {
        services.AddScoped<IClaimPartitionOwnershipService, ClaimPartitionOwnershipService>();
        services.AddScoped<IFetchService, FetchService>();
        services.AddScoped<ICommitService, CommitService>();
        services.AddScoped<ILeaveService, LeaveService>();
        
        return services;
    }
}