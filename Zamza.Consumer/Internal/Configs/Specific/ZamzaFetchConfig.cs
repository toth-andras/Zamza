namespace Zamza.Consumer.Internal.Configs.Specific;

internal sealed record ZamzaFetchConfig
{
    public int KafkaConsumesPerZamzaFetch { get; }
    public int FetchLimit { get; }

    public ZamzaFetchConfig(int kafkaConsumesPerZamzaFetch, int fetchLimit)
    {
        ValidateKafkaConsumesPerZamzaFetch(kafkaConsumesPerZamzaFetch);
        ValidateFetchLimit(fetchLimit);
        
        KafkaConsumesPerZamzaFetch = kafkaConsumesPerZamzaFetch;
        FetchLimit = fetchLimit;
    }
    
    public static ZamzaFetchConfig Default => new(
        kafkaConsumesPerZamzaFetch: 3,
        fetchLimit: 5);

    private static void ValidateKafkaConsumesPerZamzaFetch(int number)
    {
        if (number >= 1)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(
            message: "The number of consumes from Kafka per one consumer from Zamza must be positive", 
            innerException: null);
    }
    private static void ValidateFetchLimit(int number)
    {
        if (number >= 1)
        {
            return;
        }
        
        throw new ArgumentOutOfRangeException(
            message: "The fetch limit for Zamza fetches must be positive",
            innerException: null);
    }
};