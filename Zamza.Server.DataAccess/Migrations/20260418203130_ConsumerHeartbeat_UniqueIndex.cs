using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260418203130)]
public sealed class _20260418203130_ConsumerHeartbeat_UniqueIndex : SqlMigrationBase 
{
    protected override string SqlUp =>
    """
        create unique index if not exists idx_consumer_heartbeat_unique
        on zamza.consumer_heartbeat (consumer_group, consumer_id);
    """;
    
    protected override string SqlDown =>
    """
        drop index if exists idx_consumer_heartbeat_unique;
    """;
}