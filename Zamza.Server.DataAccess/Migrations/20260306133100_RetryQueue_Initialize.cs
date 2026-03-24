using FluentMigrator;

namespace Zamza.Server.DataAccess.Migrations;

[Migration(20260306133100)]
public sealed class _20260306133100_RetryQueue_Initialize : SqlMigrationBase
{
    protected override string SqlUp => 
    """
        create table if not exists zamza.retry_queue
        (
            consumer_group text not null,
            topic text not null,
            partition integer not null,
            offset_value bigint not null,
            headers jsonb not null,
            key bytea,
            value bytea,
            timestamp timestamp with time zone not null,
            max_retries_count integer not null,
            retries_count integer not null,
            processing_deadline_utc timestamp with time zone,
            next_retry_after timestamp with time zone not null,
            version smallint not null
        );
    """;
    
    protected override string SqlDown => 
    """
        drop table if exists zamza.retry_queue;
    """;
}