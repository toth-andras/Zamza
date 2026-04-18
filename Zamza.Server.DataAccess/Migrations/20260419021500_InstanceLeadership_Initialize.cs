using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260419021500)]
public sealed class _20260419021500_InstanceLeadership_Initialize : SqlMigrationBase 
{
    protected override string SqlUp => 
    """
        create table if not exists zamza.instance_leadership
        (
            key text not null,
            instance_id text not null,
            leadership_deadline timestamp with time zone not null
        );
    """;

    protected override string SqlDown => "drop table if exists zamza.instance_leadership;";
}