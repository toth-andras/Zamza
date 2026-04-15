namespace Zamza.Consumer.Internal.ConsumptionController;

internal enum ConsumptionControllerStateEnum
{
    Stopped,
    ZamzaServerNotAvailable,
    PartitionOwnershipClaimRequired,
    ProcessKafka,
    ProcessZamza
}