namespace Zamza.Consumer.Internal.ZamzaServer.Exceptions;

internal sealed class ZamzaException : Exception
{
    public ZamzaErrorCode Code { get; }

    public ZamzaException(
        ZamzaErrorCode code,
        string? message = null,
        Exception? innerException = null) : base(message, innerException)
    {
        Code = code;
    }
}