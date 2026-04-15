using Zamza.Consumer.Internal.ConsumptionController;

namespace Zamza.Consumer.Internal.RebalanceListener;

internal sealed class KafkaConsumerGroupRebalanceListener<TKey, TValue>
{
    private IConsumptionController<TKey, TValue>? _consumptionController;

    public KafkaConsumerGroupRebalanceListener()
    {
        _consumptionController = null;
    }

    public void SetController(IConsumptionController<TKey, TValue> consumptionController)
    {
        _consumptionController = consumptionController;
    }

    public void OnRebalance()
    {
        Console.WriteLine("=================== Kafka rebalance ===================");
        _consumptionController?.OnKafkaConsumerGroupRebalance();
    }
}