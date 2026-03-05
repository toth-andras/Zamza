using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Zamza.Server.ConsumerApi.GrpcServices;

namespace Zamza.Server.ConsumerApi;

public static class EndpointRouteBuilderExtensions
{
    public static IEndpointRouteBuilder AddConsumerApiEndpoints(this IEndpointRouteBuilder applicationBuilder)
    {
        applicationBuilder.MapGrpcService<ConsumerApiV1GrpcService>();
        
        return applicationBuilder;
    }
}