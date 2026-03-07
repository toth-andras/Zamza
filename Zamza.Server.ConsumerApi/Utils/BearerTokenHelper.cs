using Grpc.Core;

namespace Zamza.Server.ConsumerApi.Utils;

public static class BearerTokenHelper
{
    public static string? GetBearerToken(Metadata headers)
    {
        const string headerName = "Authorization";
        return headers.GetValue(headerName);
    }
}