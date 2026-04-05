using System.Net;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.ConsumerApi.Interceptors;

internal sealed class ExceptionInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, 
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (Exception exception)
        {
            throw new RpcException(GetStatus(exception));
        }
    }

    private static Status GetStatus(Exception exception)
    {
        return new Status(
            GetStatusCode(exception),
            exception.Message);
    }

    private static StatusCode GetStatusCode(Exception exception)
    {
        if (exception is not ZamzaException zamzaException)
        {
            return StatusCode.Internal;
        }

        return GetStatusCodeForZamzaException(zamzaException);
    }

    private static StatusCode GetStatusCodeForZamzaException(ZamzaException zamzaException)
    {
        return zamzaException.HttpErrorCode switch
        {
            HttpStatusCode.BadRequest => StatusCode.InvalidArgument,
            HttpStatusCode.GatewayTimeout => StatusCode.DeadlineExceeded,
            HttpStatusCode.InternalServerError => StatusCode.Internal,

            _ => StatusCode.Internal
        };
    }
}