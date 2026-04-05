using System.Net;

namespace Zamza.Server.Models.Exceptions;

public abstract class ZamzaException : Exception
{
    public abstract HttpStatusCode HttpErrorCode { get; }

    protected ZamzaException(string message) : base(message) { }
    
    protected ZamzaException(string message, Exception inner) : base(message, inner) { }
}