using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260312205900)]
public sealed class _20260312205900_RetryQueue_UniqueIndex : SqlMigrationBase
{
    protected override string SqlUp => 
    """
        create unique index if not exists idx_retry_queue_unique
        on zamza.retry_queue (consumer_group, topic, partition, offset_value);
    """;
    protected override string SqlDown => 
    """
        drop index if exists idx_retry_queue_unique;                                   
    """;
}