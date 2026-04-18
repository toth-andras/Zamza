namespace Zamza.Consumer.Internal.ConsumerState;

internal enum ConsumerStateEnum
{
    Stopped,
    ZamzaServerNotAvailable,
    PartitionOwnershipClaimRequired,
    ProcessKafka,
    ProcessZamza
}