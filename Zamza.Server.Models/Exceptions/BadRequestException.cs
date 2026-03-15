using System.Net;

namespace Zamza.Server.Models.Exceptions;

public sealed class BadRequestException : ZamzaException
{
    public override int HttpErrorCode => (int)HttpStatusCode.BadRequest;

    public BadRequestException(string message) : base(message) {}
}