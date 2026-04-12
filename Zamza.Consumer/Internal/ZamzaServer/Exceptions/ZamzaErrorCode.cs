namespace Zamza.Consumer.Internal.ZamzaServer.Exceptions;

internal enum ZamzaErrorCode
{
    InternalError = 1,
    ServerUnavailable = 2,
    PartitionOwnershipObsolete = 3
}