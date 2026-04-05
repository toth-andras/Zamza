using System.Net;

namespace Zamza.Server.Models.Exceptions;

public sealed class InternalException : ZamzaException
{
    public override HttpStatusCode HttpErrorCode => HttpStatusCode.InternalServerError;
    
    public InternalException(string message) : base(message) { }

    public InternalException(string message, Exception inner) : base(message, inner) { }
}