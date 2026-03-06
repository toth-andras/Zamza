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
            headers jsonb,
            key bytea,
            value bytea,
            timestamp timestamp with time zone not null,
            max_retries integer not null,
            min_retries_gap_ms bigint not null,
            processing_period_ms bigint,
            
            retries_count integer not null,
            next_retry_after timestamp with time zone not null,
            processing_deadline timestamp with time zone,
            last_retry_at_utc timestamp,
            version smallint not null
        );
    """;
    
    protected override string SqlDown => 
    """
        drop table if exists zamza.retry_queue;
    """;
}