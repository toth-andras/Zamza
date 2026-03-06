using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260306132300)]
public sealed class _20260306132300_PartitionOwnership_Initialize : SqlMigrationBase
{
    protected override string SqlUp => 
    """
        create table if not exists zamza.partition_ownership
        (
            consumer_group text not null,
            topic text not null,
            partition integer not null,
            epoch bigint not null,
            consumer_id text,
            timestamp_utc timestamp with time zone not null
        );
    """;
    protected override string SqlDown => 
    """
        drop table if exists zamza.partition_ownership;                             
    """;
}