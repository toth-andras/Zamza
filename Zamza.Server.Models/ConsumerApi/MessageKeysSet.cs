namespace Zamza.Server.Models.ConsumerApi;

public sealed record MessageKeysSet
{
    public string ConsumerGroup { get; }
    public string[] TopicValue { get; }
    public int[] PartitionValue { get; }
    public long[] OffsetValue { get; }
    
    public int MessageCount => TopicValue.Length;

    public MessageKeysSet(
        string consumerGroup, 
        string[] topicValue, 
        int[] partitionValue, 
        long[] offsetValue)
    {
        ArgumentNullException.ThrowIfNull(consumerGroup);
        ThrowIfArraysAreNotOfTheSameSize(topicValue, partitionValue, offsetValue);
        
        ConsumerGroup = consumerGroup;
        TopicValue = topicValue;
        PartitionValue = partitionValue;
        OffsetValue = offsetValue;
    }

    public (string Topic, int Partition) GetPartitionForMessage(int messageIndex)
    {
        if (messageIndex < 0 || messageIndex >= MessageCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(messageIndex),
                message: "No message with such index in message keys set");
        }
        return (TopicValue[messageIndex], PartitionValue[messageIndex]);
    }

    private static void ThrowIfArraysAreNotOfTheSameSize(
        params Array[] arrays)
    {
        var lenght = arrays[0].Length;
        for (int i = 1; i < lenght; i++)
        {
            if (arrays[i].Length != lenght)
            {
                throw new Exception("Message key data arrays are not of the same size");
            }
        }
    }
}