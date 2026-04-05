using System.Net;

namespace Zamza.Server.Models.Exceptions;

public sealed class TimeoutException : ZamzaException
{
    public override HttpStatusCode HttpErrorCode => HttpStatusCode.GatewayTimeout;
    
    public TimeoutException(string message) : base(message) { }

    public TimeoutException(string message, Exception inner) : base(message, inner) { }
}