using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260418195430)]
public sealed class _20260418195430_ConsumerHeartbeat_Initialize : SqlMigrationBase
{
    protected override string SqlUp => 
    """
        create table if not exists zamza.consumer_heartbeat
        (
            consumer_group text not null,
            consumer_id text not null,
            timestamp_utc timestamp with time zone not null
        );
    """;
    
    protected override string SqlDown => 
    """
        drop table if exists zamza.consumer_heartbeat;
    """;
}