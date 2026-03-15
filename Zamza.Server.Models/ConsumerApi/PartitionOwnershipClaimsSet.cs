using System.Collections;
using Zamza.Server.Models.Exceptions;

namespace Zamza.Server.Models.ConsumerApi;

public sealed record PartitionOwnershipClaimsSet 
    : IEnumerable<((string Topic, int PartitionNumber) Partition, long KnownOwnershipEpoch)>
{
    private readonly string[] _topicValues;
    private readonly int[] _partitionValues;
    private readonly long[] _knownOwnershipEpochValues;
    
    public string ConsumerId { get; }
    public string ConsumerGroup { get; }
    public int ClaimsCount => _topicValues.Length;
    
    public PartitionOwnershipClaimsSet(
        string consumerId,
        string consumerGroup,
        string[] topicValues,
        int[] partitionValues,
        long[] knownOwnershipEpochValues)
    {
        ThrowIfArrayLengthsAreNotEqual(topicValues, partitionValues, knownOwnershipEpochValues);
        
        ConsumerId = consumerId;
        ConsumerGroup = consumerGroup;
        _topicValues = topicValues;
        _partitionValues = partitionValues;
        _knownOwnershipEpochValues = knownOwnershipEpochValues;
    }

    public (string[] TopicValues, int[] ParitionValues, long[] KnownOwnershipEpochValues) ToDataArrays()
    {
        return (_topicValues, _partitionValues, _knownOwnershipEpochValues);
    }

    public IEnumerator<((string Topic, int PartitionNumber) Partition, long KnownOwnershipEpoch)> GetEnumerator()
    {
        for (var i = 0; i < ClaimsCount; i++)
        {
            yield return ((_topicValues[i], _partitionValues[i]), _knownOwnershipEpochValues[i]);
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
    
    private static void ThrowIfArrayLengthsAreNotEqual(params Array[] arrays)
    {
        var size = arrays[0].Length;
        for (var index = 1; index < arrays.Length; index++)
        {
            if (arrays[index].Length != size)
            {
                throw new BadRequestException("The sizes of data arrays for partition ownership claims does not match.");
            }
        }
    }
}