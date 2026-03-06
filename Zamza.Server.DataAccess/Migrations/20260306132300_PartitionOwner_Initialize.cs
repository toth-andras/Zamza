using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260306132300)]
public sealed class _20260306132300_PartitionOwner_Initialize : SqlMigrationBase
{
    protected override string SqlUp => 
    """
        create table if not exists zamza.partition_owner
        (
            consumer_group text not null,
            topic text not null,
            partition integer not null,
            epoch bigint not null,
            consumer_id text,
            timestamp timestamp not null
        );
    """;
    protected override string SqlDown => 
    """
        drop table if exists zamza.partition_owner;                             
    """;
}