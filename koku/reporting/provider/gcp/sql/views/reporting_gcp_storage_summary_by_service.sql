DROP INDEX IF EXISTS gcp_storage_summary_service;
DROP MATERIALIZED VIEW IF EXISTS reporting_gcp_storage_summary_by_service;

CREATE MATERIALIZED VIEW reporting_gcp_storage_summary_by_service AS (
    SELECT ROW_NUMBER() OVER(ORDER BY usage_start, account_id, service_id, service_alias) AS id,
        usage_start,
        usage_start as usage_end,
        sum(usage_amount) as usage_amount,
        MAX(unit) AS unit,
        SUM(unblended_cost) AS unblended_cost,
        SUM(markup_cost) AS markup_cost,
        MAX(currency) AS currency,
        account_id,
        service_id,
        service_alias,
        max(source_uuid::text)::uuid as source_uuid
    FROM reporting_gcpcostentrylineitem_daily_summary
    WHERE usage_start >= DATE_TRUNC('month', NOW() - '2 month'::interval)::date
        AND service_alias IN ('Filestore', 'Storage', 'Cloud Storage', 'Data Transfer')
    GROUP BY usage_start, account_id, service_id, service_alias
)
WITH DATA
    ;

CREATE UNIQUE INDEX gcp_storage_summary_service
    ON reporting_gcp_storage_summary_by_service (usage_start, account_id, service_id, service_alias)
;
