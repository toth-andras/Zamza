using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Zamza.Server.UserApi.Middlewares;

namespace Zamza.Server.UserApi;

public static class UserApiWebApplicationExtensions
{
    public static IEndpointRouteBuilder AddUserApiEndpoints(this WebApplication endpoints)
    {
        endpoints.UseMiddleware<ErrorHandlingMiddleware>();
        
        endpoints.UseSwagger();
        endpoints.UseSwaggerUI();
        
        endpoints.MapControllers();
        
        return endpoints;
    }
}