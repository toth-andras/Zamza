namespace Zamza.Consumer.Internal.ConsumptionController;

internal enum ConsumerStateEnum
{
    Stopped,
    ZamzaServerNotAvailable,
    PartitionOwnershipClaimRequired,
    ProcessKafka,
    ProcessZamza
}