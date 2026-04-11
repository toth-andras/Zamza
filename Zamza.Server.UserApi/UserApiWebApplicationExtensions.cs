using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

namespace Zamza.Server.UserApi;

public static class UserApiWebApplicationExtensions
{
    public static IEndpointRouteBuilder AddUserApiEndpoints(this WebApplication endpoints)
    {
        endpoints.UseSwagger();
        endpoints.UseSwaggerUI();
        
        endpoints.MapControllers();
        
        return endpoints;
    }
}