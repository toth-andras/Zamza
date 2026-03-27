using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260306160600)]
public class _20260306160600_Dlq_Initialize : SqlMigrationBase
{
    protected override string SqlUp =>
    """
        create table if not exists zamza.dlq
        (
            id bigint generated always as identity primary key,
            consumer_group text not null,
            topic text not null,
            partition integer not null,
            offset_value bigint not null,
            headers jsonb not null,
            key bytea,
            value bytea,
            timestamp timestamp with time zone not null,
            retries_count int not null,
            became_poisoned_at_utc timestamp with time zone not null,
            version smallint not null
        );                                           
    """;
    protected override string SqlDown => 
    """
        drop table if exists zamza.dlq;
    """;
}