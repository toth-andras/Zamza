using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260419022000)]
public sealed class _20260419022000_InstanceLeadership_UniqueIndex : SqlMigrationBase 
{
    protected override string SqlUp =>
    """
        create unique index if not exists idx_instance_leadership_unique
        on zamza.instance_leadership (key, instance_id);
    """;

    protected override string SqlDown => "drop index if exists idx_instance_leadership_unique;";
}