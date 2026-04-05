using System.Net;

namespace Zamza.Server.Models.Exceptions;

public sealed class BadRequestException : ZamzaException
{
    public override HttpStatusCode HttpErrorCode => HttpStatusCode.BadRequest;

    public BadRequestException(string message) : base(message) {}
}