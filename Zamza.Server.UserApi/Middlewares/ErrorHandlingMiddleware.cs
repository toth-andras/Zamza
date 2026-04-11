using System.Net;
using Microsoft.AspNetCore.Http;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.UserApi.Middlewares;

internal sealed class ErrorHandlingMiddleware : IMiddleware
{
    public async Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        try
        {
            await next(context);
        }
        catch (Exception exception)
        {
            var errorInfo = GetErrorInfo(exception);

            context.Response.StatusCode = errorInfo.HttpCode;
            context.Response.ContentType = "text/plain";
            await context.Response.WriteAsync(errorInfo.Message);
        }
    }

    private static ErrorInfo GetErrorInfo(Exception exception)
    {
        if (exception is ZamzaException zamzaException)
        {
            return GetErrorInfoFromZamzaException(zamzaException);
        }

        return new ErrorInfo(
            (int) HttpStatusCode.InternalServerError,
            exception.Message);
    }

    private static ErrorInfo GetErrorInfoFromZamzaException(ZamzaException exception)
    {
        return new ErrorInfo(
            (int)exception.HttpErrorCode,
            exception.Message);
    }

    private sealed record ErrorInfo(
        int HttpCode,
        string Message);
}