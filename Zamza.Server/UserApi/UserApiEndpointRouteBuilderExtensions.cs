namespace Zamza.Server.UserApi;

public static class UserApiEndpointRouteBuilderExtensions
{
    public static IEndpointRouteBuilder AddUserApiEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapControllers();
        
        return endpoints;
    }
}