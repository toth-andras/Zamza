using System.Net;

namespace Zamza.Server.Models.Exceptions;

public sealed class TimeoutException : ZamzaException
{
    public override int HttpErrorCode => (int)HttpStatusCode.GatewayTimeout;
    
    public TimeoutException(string message) : base(message) { }

    public TimeoutException(string message, Exception inner) : base(message, inner) { }
}