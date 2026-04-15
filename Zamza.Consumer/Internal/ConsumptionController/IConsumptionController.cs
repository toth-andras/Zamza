namespace Zamza.Consumer.Internal.ConsumptionController;

public interface IConsumptionController<TKey, TValue>
{
    Task RunMainLoop(CancellationToken token);

    void OnKafkaConsumerGroupRebalance();
}