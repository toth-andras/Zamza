using Dapper;
using Zamza.Server.DataAccess.Common.Connections;
using Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository.SqlCommands;
using Zamza.Server.Models.ConsumerApi;

namespace Zamza.Server.DataAccess.Repositories.PartitionOwnershipRepository;

internal sealed class PartitionOwnershipRepository : IPartitionOwnershipRepository
{
    private readonly IConnectionFactory  _connectionFactory;

    public PartitionOwnershipRepository(IConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<IReadOnlyDictionary<(string Topic, int Partition), PartitionOwnership>> Get(
        string consumerGroup, 
        CancellationToken cancellation)
    {
        await using var connection = await _connectionFactory.CreateConnection(cancellation);
        
        var sqlCommand = GetPartitionOwnershipsForConsumerGroupSqlCommand.BuildCommandDefinition(
            consumerGroup,
            cancellation);

        return (await connection.QueryAsync<PartitionOwnership>(sqlCommand)).ToDictionary(
            partitionOwnership => (partitionOwnership.Topic, partitionOwnership.Partition),
            partitionOwnership => partitionOwnership);
    }
}