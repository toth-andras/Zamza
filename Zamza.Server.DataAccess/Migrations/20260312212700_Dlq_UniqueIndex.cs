using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260312212700)]
public sealed class _20260312212700_Dlq_UniqueIndex : SqlMigrationBase
{
    protected override string SqlUp => 
    """
        create unique index if not exists idx_dlq_unique
        on zamza.dlq (consumer_group, topic, partition, offset_value);
    """;

    protected override string SqlDown => 
    """
        drop index if exists idx_dlq_unique;                                   
    """;
}