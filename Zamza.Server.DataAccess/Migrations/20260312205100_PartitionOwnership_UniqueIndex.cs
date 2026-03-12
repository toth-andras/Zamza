using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260312205100)]
public sealed class _20260312205100_PartitionOwnership_UniqueIndex : SqlMigrationBase
{
    protected override string SqlUp =>
    """
        create unique index if not exists idx_partition_ownership_unique
        on zamza.partition_ownership (consumer_group, topic, partition);
    """;

    protected override string SqlDown =>
    """
        drop index if exists idx_partition_ownership_unique;                                     
    """;
}