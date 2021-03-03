-- {{schema | sqlsafe}}
-- {{uuid | sqlsafe}}

DROP TABLE IF EXISTS hive.acct10001.__matched_tags_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__matched_tags_00000000_0000_0000_0000_000000000000 AS (
    WITH cte_unnested_gcp_tags AS (
        SELECT tags.*,
            b.billing_period_start
        FROM (
            SELECT key,
                value,
                cost_entry_bill_id
            FROM postgres.acct10001.reporting_gcptags_summary AS ts
            CROSS JOIN UNNEST("values") AS v(value)
        ) AS tags
        JOIN postgres.acct10001.reporting_gcpcostentrybill AS b
            ON tags.cost_entry_bill_id = b.id
        JOIN postgres.acct10001.reporting_gcpenabledtagkeys as enabled_tags
            ON lower(enabled_tags.key) = lower(tags.key)
        WHERE b.id = 3 -- {{bill_id}}
    ),
    cte_unnested_ocp_pod_tags AS (
        SELECT tags.*,
            rp.report_period_start,
            rp.cluster_id,
            rp.cluster_alias
        FROM (
            SELECT key,
                value,
                report_period_id
            FROM postgres.acct10001.reporting_ocpusagepodlabel_summary AS ts
            CROSS JOIN UNNEST("values") AS v(value)
        ) AS tags
        JOIN postgres.acct10001.reporting_ocpusagereportperiod AS rp
            ON tags.report_period_id = rp.id
        -- Filter out tags that aren't enabled
        JOIN postgres.acct10001.reporting_ocpenabledtagkeys as enabled_tags
            ON lower(enabled_tags.key) = lower(tags.key)
        WHERE rp.cluster_id = 'my-ocp-cluster-gcp1' -- {{cluster_id}}
    ),
    cte_unnested_ocp_volume_tags AS (
        SELECT tags.*,
            rp.report_period_start,
            rp.cluster_id,
            rp.cluster_alias
        FROM (
            SELECT key,
                value,
                report_period_id
            FROM postgres.acct10001.reporting_ocpstoragevolumelabel_summary AS ts
            CROSS JOIN UNNEST("values") AS v(value)
        ) AS tags
        JOIN postgres.acct10001.reporting_ocpusagereportperiod AS rp
            ON tags.report_period_id = rp.id
        -- Filter out tags that aren't enabled
        JOIN postgres.acct10001.reporting_ocpenabledtagkeys as enabled_tags
            ON lower(enabled_tags.key) = lower(tags.key)
        WHERE rp.cluster_id = 'my-ocp-cluster-gcp1' -- {{cluster_id}}
    )
    SELECT '{"' || key || '": "' || value || '"}' as tag,
        key,
        value,
        cost_entry_bill_id,
        report_period_id
    FROM (
        SELECT gcp.key,
            gcp.value,
            gcp.cost_entry_bill_id,
            ocp.report_period_id
        FROM cte_unnested_gcp_tags AS gcp
        JOIN cte_unnested_ocp_pod_tags AS ocp
            ON lower(gcp.key) = lower(ocp.key)
                AND lower(gcp.value) = lower(ocp.value)
                AND gcp.billing_period_start = ocp.report_period_start

        UNION

        SELECT gcp.key,
            gcp.value,
            gcp.cost_entry_bill_id,
            ocp.report_period_id
        FROM cte_unnested_gcp_tags AS gcp
        JOIN cte_unnested_ocp_volume_tags AS ocp
            ON lower(gcp.key) = lower(ocp.key)
                AND lower(gcp.value) = lower(ocp.value)
                AND gcp.billing_period_start = ocp.report_period_start
    ) AS matches
)
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_gcp_daily_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_gcp_daily_00000000_0000_0000_0000_000000000000 AS (
    WITH cte_line_items AS (
        SELECT {{bill_id | sqlsafe}} as cost_entry_bill_id,
            cast(uuid() as varchar) as line_item_id,
            date(coalesce(date, usagedatetime)) as usage_date,
            coalesce(subscriptionid, subscriptionguid) as subscription_guid,
            json_extract_scalar(json_parse(gcp.additionalinfo), '$.ServiceType') as instance_type,
            coalesce(servicename, metercategory) as service_name,
            resourcelocation as resource_location,
            split_part(coalesce(resourceid, instanceid), '/', 9) as resource_id,
            cast(coalesce(quantity, usagequantity) as decimal(24,9)) as usage_quantity,
            cast(coalesce(costinbillingcurrency, pretaxcost) as decimal(24,9)) as pretax_cost,
            coalesce(billingcurrencycode, currency) as currency,
            CASE
                WHEN split_part(unitofmeasure, ' ', 2) != '' AND NOT (unitofmeasure = '100 Hours' AND metercategory='Virtual Machines')
                    THEN cast(split_part(unitofmeasure, ' ', 1) as integer)
                ELSE 1
                END as multiplier,
            CASE
                WHEN split_part(unitofmeasure, ' ', 2) = 'Hours'
                    THEN  'Hrs'
                WHEN split_part(unitofmeasure, ' ', 2) = 'GB/Month'
                    THEN  'GB-Mo'
                WHEN split_part(unitofmeasure, ' ', 2) != ''
                    THEN  split_part(unitofmeasure, ' ', 2)
                ELSE unitofmeasure
            END as unit_of_measure,
            tags,
            lower(tags) as lower_tags
        FROM hive.acct10001.gcp_line_items as gcp
        WHERE gcp.source = '4963ab4d-a830-4226-ba3c-faeb9cf76f3e' -- '{{gcp_source_uuid | sqlsafe}}'
            AND gcp.year = '2021' -- '{{year | sqlsafe}}'
            AND gcp.month = '03' -- '{{month | sqlsafe}}'
            AND date(coalesce(date, usagedatetime)) >= date('2021-03-01'::timestamp) -- date('{{start_date | sqlsafe}}')
            AND date(coalesce(date, usagedatetime)) <= date('2021-04-01'::timestamp) -- date('{{end_date | sqlsafe}}')
    )
    SELECT gcp.cost_entry_bill_id,
        gcp.line_item_id,
        gcp.usage_date,
        gcp.subscription_guid,
        gcp.instance_type,
        gcp.service_name,
        gcp.resource_location,
        gcp.resource_id,
        gcp.usage_quantity * gcp.multiplier as usage_quantity,
        gcp.pretax_cost,
        gcp.currency,
        gcp.unit_of_measure,
        gcp.tags,
        gcp.lower_tags
    FROM cte_line_items AS gcp
)
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_gcp_tags_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_gcp_tags_00000000_0000_0000_0000_000000000000 AS (
    SELECT gcp.*
    FROM (
        SELECT gcp.*,
            row_number() OVER (PARTITION BY gcp.line_item_id ORDER BY gcp.line_item_id) as row_number
        FROM hive.acct10001.__reporting_gcp_daily_00000000_0000_0000_0000_000000000000 as gcp
        JOIN hive.acct10001.__matched_tags_00000000_0000_0000_0000_000000000000 as tag
            ON json_extract_scalar(gcp.tags, '$.' || tag.key) = tag.value
    ) AS gcp
    WHERE gcp.row_number = 1
)
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000 AS (
    SELECT gcp.*
    FROM hive.acct10001.__reporting_gcp_daily_00000000_0000_0000_0000_000000000000 as gcp
    WHERE (
        strpos(lower_tags, 'openshift_cluster') != 0
        OR strpos(lower_tags, 'openshift_node') != 0
        OR strpos(lower_tags, 'openshift_project') != 0
    )
)
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_ocp_storage_tags_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_ocp_storage_tags_00000000_0000_0000_0000_000000000000 AS (
    SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
        ocp.usage_start,
        ocp.report_period_id,
        ocp.cluster_id,
        ocp.cluster_alias,
        ocp.namespace,
        ocp.node,
        ocp.persistentvolumeclaim,
        ocp.persistentvolume,
        ocp.storageclass,
        ocp.persistentvolumeclaim_capacity_gigabyte,
        ocp.persistentvolumeclaim_capacity_gigabyte_months,
        ocp.volume_request_storage_gigabyte_months,
        ocp.persistentvolumeclaim_usage_gigabyte_months,
        json_format(ocp.volume_labels) as volume_labels,
        lower(tag.key) as key,
        lower(tag.value) as value,
        lower(tag.tag) as tag
    FROM postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.acct10001.__matched_tags_00000000_0000_0000_0000_000000000000 AS tag
        ON ocp.report_period_id = tag.report_period_id
        AND json_extract_scalar(ocp.volume_labels, '$.' || tag.key) = tag.value
    WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
        AND ocp.data_source = 'Storage'
        AND date(ocp.usage_start) >= date('2021-03-01'::timestamp) -- date('{{start_date | sqlsafe}}')
        AND date(ocp.usage_start) <= date('2021-04-01'::timestamp) -- date('{{end_date | sqlsafe}}')
)
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_ocp_pod_tags_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_ocp_pod_tags_00000000_0000_0000_0000_000000000000 AS (
    SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
        ocp.usage_start,
        ocp.report_period_id,
        ocp.cluster_id,
        ocp.cluster_alias,
        ocp.namespace,
        ocp.node,
        json_format(ocp.pod_labels) as pod_labels,
        ocp.pod_usage_cpu_core_hours,
        ocp.pod_request_cpu_core_hours,
        ocp.pod_limit_cpu_core_hours,
        ocp.pod_usage_memory_gigabyte_hours,
        ocp.pod_request_memory_gigabyte_hours,
        ocp.node_capacity_cpu_cores,
        ocp.node_capacity_cpu_core_hours,
        ocp.node_capacity_memory_gigabytes,
        ocp.node_capacity_memory_gigabyte_hours,
        ocp.cluster_capacity_cpu_core_hours,
        ocp.cluster_capacity_memory_gigabyte_hours,
        lower(tag.key) as key,
        lower(tag.value) as value,
        lower(tag.tag) as tag
    FROM postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
    JOIN hive.acct10001.__matched_tags_00000000_0000_0000_0000_000000000000 AS tag
        ON ocp.report_period_id = tag.report_period_id
        AND json_extract_scalar(ocp.pod_labels, '$.' || tag.key) = tag.value
    WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
        AND ocp.data_source = 'Pod'
        AND date(ocp.usage_start) >= date('2021-03-01'::timestamp) -- date('{{start_date | sqlsafe}}')
        AND date(ocp.usage_start) <= date('2021-04-01'::timestamp) -- date('{{end_date | sqlsafe}}')
)
;



-- First we match OCP pod data to gcp data using a direct
-- resource id match. This usually means OCP node -> gcp EC2 instance ID.
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS (
    WITH cte_resource_id_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            json_format(ocp.pod_labels) as pod_labels,
            ocp.pod_usage_cpu_core_hours,
            ocp.pod_request_cpu_core_hours,
            ocp.pod_limit_cpu_core_hours,
            ocp.pod_usage_memory_gigabyte_hours,
            ocp.pod_request_memory_gigabyte_hours,
            ocp.node_capacity_cpu_cores,
            ocp.node_capacity_cpu_core_hours,
            ocp.node_capacity_memory_gigabytes,
            ocp.node_capacity_memory_gigabyte_hours,
            ocp.cluster_capacity_cpu_core_hours,
            ocp.cluster_capacity_memory_gigabyte_hours,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_daily_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            -- NOTE: We would normally use ocp.resource_id
            -- For this JOIN, but it is not guaranteed to be correct
            -- in the current Operator Metering version
            -- so we are matching only on the node name
            -- which should match the split gcp instance ID
            ON gcp.resource_id = ocp.node
                AND gcp.usage_date = ocp.usage_start
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.usage_start >= date('2021-03-01'::timestamp) -- date('{{start_date | sqlsafe}}')
            AND ocp.usage_start <= date('2021-04-01'::timestamp) -- date('{{end_date | sqlsafe}}')
            AND ocp.data_source = 'Pod'
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_resource_id_matched
        GROUP BY gcp_id
    )
    SELECT rm.*,
        (rm.pod_usage_cpu_core_hours / rm.node_capacity_cpu_core_hours) * rm.pretax_cost as project_cost,
        shared.shared_projects
    FROM cte_resource_id_matched AS rm
    JOIN cte_number_of_shared AS shared
        ON rm.gcp_id = shared.gcp_id
)
;

-- Next we match where the gcp tag is the special openshift_project key
-- and the value matches an OpenShift project name
INSERT INTO hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            json_format(ocp.pod_labels) as pod_labels,
            ocp.pod_usage_cpu_core_hours,
            ocp.pod_request_cpu_core_hours,
            ocp.pod_limit_cpu_core_hours,
            ocp.pod_usage_memory_gigabyte_hours,
            ocp.pod_request_memory_gigabyte_hours,
            ocp.node_capacity_cpu_cores,
            ocp.node_capacity_cpu_core_hours,
            ocp.node_capacity_memory_gigabytes,
            ocp.node_capacity_memory_gigabyte_hours,
            ocp.cluster_capacity_cpu_core_hours,
            ocp.cluster_capacity_memory_gigabyte_hours,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            ON json_extract_scalar(gcp.lower_tags, '$.openshift_project') = lower(ocp.namespace)
                AND gcp.usage_date = ocp.usage_start
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.data_source = 'Pod'
            AND rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
;

-- Next we match where the gcp tag is the special openshift_node key
-- and the value matches an OpenShift node name
INSERT INTO hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            json_format(ocp.pod_labels) as pod_labels,
            ocp.pod_usage_cpu_core_hours,
            ocp.pod_request_cpu_core_hours,
            ocp.pod_limit_cpu_core_hours,
            ocp.pod_usage_memory_gigabyte_hours,
            ocp.pod_request_memory_gigabyte_hours,
            ocp.node_capacity_cpu_cores,
            ocp.node_capacity_cpu_core_hours,
            ocp.node_capacity_memory_gigabytes,
            ocp.node_capacity_memory_gigabyte_hours,
            ocp.cluster_capacity_cpu_core_hours,
            ocp.cluster_capacity_memory_gigabyte_hours,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            ON json_extract_scalar(gcp.lower_tags, '$.openshift_node') = lower(ocp.node)
                AND gcp.usage_date = ocp.usage_start
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.data_source = 'Pod'
            AND rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
;

-- Next we match where the gcp tag is the special openshift_cluster key
-- and the value matches an OpenShift cluster name
 INSERT INTO hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            json_format(ocp.pod_labels) as pod_labels,
            ocp.pod_usage_cpu_core_hours,
            ocp.pod_request_cpu_core_hours,
            ocp.pod_limit_cpu_core_hours,
            ocp.pod_usage_memory_gigabyte_hours,
            ocp.pod_request_memory_gigabyte_hours,
            ocp.node_capacity_cpu_cores,
            ocp.node_capacity_cpu_core_hours,
            ocp.node_capacity_memory_gigabytes,
            ocp.node_capacity_memory_gigabyte_hours,
            ocp.cluster_capacity_cpu_core_hours,
            ocp.cluster_capacity_memory_gigabyte_hours,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            ON json_extract_scalar(gcp.lower_tags, '$.openshift_cluster') IN (lower(ocp.cluster_id), lower(ocp.cluster_alias))
                AND gcp.usage_date = ocp.usage_start
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.data_source = 'Pod'
            AND rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
;

-- Next we match where the pod label key and value
-- and gcp tag key and value match directly
 INSERT INTO hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT ocp.ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            ocp.pod_labels,
            ocp.pod_usage_cpu_core_hours,
            ocp.pod_request_cpu_core_hours,
            ocp.pod_limit_cpu_core_hours,
            ocp.pod_usage_memory_gigabyte_hours,
            ocp.pod_request_memory_gigabyte_hours,
            ocp.node_capacity_cpu_cores,
            ocp.node_capacity_cpu_core_hours,
            ocp.node_capacity_memory_gigabytes,
            ocp.node_capacity_memory_gigabyte_hours,
            ocp.cluster_capacity_cpu_core_hours,
            ocp.cluster_capacity_memory_gigabyte_hours,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN hive.acct10001.__reporting_ocp_pod_tags_00000000_0000_0000_0000_000000000000 as ocp
            ON gcp.usage_date = ocp.usage_start
                AND strpos(gcp.lower_tags, ocp.tag) != 0
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_ocp_pod_tags_00000000_0000_0000_0000_000000000000;

-- First we match OCP storage data to gcp data using a direct
-- resource id match. OCP PVC name -> gcp instance ID.
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 AS (
    WITH cte_tag_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            ocp.persistentvolumeclaim,
            ocp.persistentvolume,
            ocp.storageclass,
            ocp.persistentvolumeclaim_capacity_gigabyte,
            ocp.persistentvolumeclaim_capacity_gigabyte_months,
            ocp.volume_request_storage_gigabyte_months,
            ocp.persistentvolumeclaim_usage_gigabyte_months,
            json_format(ocp.volume_labels) as volume_labels,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_daily_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            ON gcp.resource_id LIKE '%%' || ocp.persistentvolume
                AND gcp.usage_date = ocp.usage_start
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS ulid
            ON ulid.gcp_id = gcp.line_item_id
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.data_source = 'Storage'
            AND ocp.usage_start >= date('2021-03-01'::timestamp) -- date('{{start_date | sqlsafe}}')
            AND ocp.usage_start <= date('2021-04-01'::timestamp) -- date('{{end_date | sqlsafe}}')
            AND ulid.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
)
;


-- Next we match where the gcp tag is the special openshift_project key
-- and the value matches an OpenShift project name
INSERT INTO hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            ocp.persistentvolumeclaim,
            ocp.persistentvolume,
            ocp.storageclass,
            ocp.persistentvolumeclaim_capacity_gigabyte,
            ocp.persistentvolumeclaim_capacity_gigabyte_months,
            ocp.volume_request_storage_gigabyte_months,
            ocp.persistentvolumeclaim_usage_gigabyte_months,
            json_format(ocp.volume_labels) as volume_labels,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            ON json_extract_scalar(gcp.lower_tags, '$.openshift_project') = lower(ocp.namespace)
                AND gcp.usage_date = ocp.usage_start
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS ulid
            ON ulid.gcp_id = gcp.line_item_id
        LEFT JOIN hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.data_source = 'Storage'
            AND ocp.usage_start >= date('2021-03-01'::timestamp) -- date('{{start_date | sqlsafe}}')
            AND ocp.usage_start <= date('2021-04-01'::timestamp) -- date('{{end_date | sqlsafe}}')
            AND ulid.gcp_id IS NULL
            AND rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
;

-- Next we match where the gcp tag is the special openshift_node key
-- and the value matches an OpenShift node name
INSERT INTO hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            ocp.persistentvolumeclaim,
            ocp.persistentvolume,
            ocp.storageclass,
            ocp.persistentvolumeclaim_capacity_gigabyte,
            ocp.persistentvolumeclaim_capacity_gigabyte_months,
            ocp.volume_request_storage_gigabyte_months,
            ocp.persistentvolumeclaim_usage_gigabyte_months,
            json_format(ocp.volume_labels) as volume_labels,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            ON json_extract_scalar(gcp.lower_tags, '$.openshift_node') = lower(ocp.node)
                AND gcp.usage_date = ocp.usage_start
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS ulid
            ON ulid.gcp_id = gcp.line_item_id
        LEFT JOIN hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.data_source = 'Storage'
            AND ulid.gcp_id IS NULL
            AND rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
;

-- Next we match where the gcp tag is the special openshift_cluster key
-- and the value matches an OpenShift cluster name
 INSERT INTO hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT cast(ocp.uuid AS VARCHAR) AS ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            ocp.persistentvolumeclaim,
            ocp.persistentvolume,
            ocp.storageclass,
            ocp.persistentvolumeclaim_capacity_gigabyte,
            ocp.persistentvolumeclaim_capacity_gigabyte_months,
            ocp.volume_request_storage_gigabyte_months,
            ocp.persistentvolumeclaim_usage_gigabyte_months,
            json_format(ocp.volume_labels) as volume_labels,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN postgres.acct10001.reporting_ocpusagelineitem_daily_summary as ocp
            ON json_extract_scalar(gcp.lower_tags, '$.openshift_cluster') IN (lower(ocp.cluster_id), lower(ocp.cluster_alias))
                AND gcp.usage_date = ocp.usage_start
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS ulid
            ON ulid.gcp_id = gcp.line_item_id
        LEFT JOIN hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE ocp.source_uuid = UUID '{{ocp_source_uuid | sqlsafe}}'
            AND ocp.data_source = 'Storage'
            AND ulid.gcp_id IS NULL
            AND rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id

;

DROP TABLE IF EXISTS hive.acct10001.__reporting_gcp_daily_00000000_0000_0000_0000_000000000000
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_gcp_special_case_tags_00000000_0000_0000_0000_000000000000
;


-- Then we match for OpenShift volume data where the volume label key and value
-- and gcp tag key and value match directly
 INSERT INTO hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000
    WITH cte_tag_matched AS (
        SELECT ocp.ocp_id,
            ocp.report_period_id,
            ocp.cluster_id,
            ocp.cluster_alias,
            ocp.namespace,
            ocp.node,
            ocp.persistentvolumeclaim,
            ocp.persistentvolume,
            ocp.storageclass,
            ocp.persistentvolumeclaim_capacity_gigabyte,
            ocp.persistentvolumeclaim_capacity_gigabyte_months,
            ocp.volume_request_storage_gigabyte_months,
            ocp.persistentvolumeclaim_usage_gigabyte_months,
            ocp.volume_labels,
            gcp.cost_entry_bill_id,
            gcp.line_item_id as gcp_id,
            gcp.usage_date,
            gcp.subscription_guid,
            gcp.instance_type,
            gcp.service_name,
            gcp.resource_location,
            gcp.resource_id,
            gcp.usage_quantity,
            gcp.pretax_cost,
            gcp.currency,
            gcp.unit_of_measure,
            gcp.tags
        FROM hive.acct10001.__reporting_gcp_tags_00000000_0000_0000_0000_000000000000 as gcp
        JOIN hive.acct10001.__reporting_ocp_storage_tags_00000000_0000_0000_0000_000000000000 as ocp
            ON gcp.usage_date = ocp.usage_start
                AND strpos(gcp.lower_tags, ocp.tag) != 0
        -- ANTI JOIN to remove rows that already matched
        LEFT JOIN hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 AS rm
            ON rm.gcp_id = gcp.line_item_id
        WHERE rm.gcp_id IS NULL
    ),
    cte_number_of_shared AS (
        SELECT gcp_id,
            count(DISTINCT namespace) as shared_projects
        FROM cte_tag_matched
        GROUP BY gcp_id
    )
    SELECT tm.*,
        tm.pretax_cost / shared.shared_projects as project_cost,
        shared.shared_projects
    FROM cte_tag_matched AS tm
    JOIN cte_number_of_shared AS shared
        ON tm.gcp_id = shared.gcp_id
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_ocp_storage_tags_00000000_0000_0000_0000_000000000000
;

DROP TABLE IF EXISTS hive.acct10001.__reporting_gcp_tags_00000000_0000_0000_0000_000000000000
;


-- The full summary data for Openshift pod<->gcp and
-- Openshift volume<->gcp matches are UNIONed together
-- with a GROUP BY using the gcp ID to deduplicate
-- the gcp data. This should ensure that we never double count
-- gcp cost or usage.
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpcostlineitem_daily_summary_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_ocpgcpcostlineitem_daily_summary_00000000_0000_0000_0000_000000000000 AS (
    WITH cte_pod_project_cost AS (
        SELECT pc.gcp_id,
            map_agg(pc.namespace, pc.project_cost) as project_costs
            FROM (
                SELECT li.gcp_id,
                    li.namespace,
                    sum(project_cost) as project_cost
                FROM hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 as li
                GROUP BY li.gcp_id, li.namespace
            ) AS pc
        GROUP BY pc.gcp_id
    ),
    cte_storage_project_cost AS (
        SELECT pc.gcp_id,
            map_agg(pc.namespace, pc.project_cost) as project_costs
        FROM (
            SELECT li.gcp_id,
                li.namespace,
                sum(project_cost) as project_cost
            FROM hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 as li
            GROUP BY li.gcp_id, li.namespace
        ) AS pc
        GROUP BY pc.gcp_id
    )
    SELECT max(li.report_period_id) as report_period_id,
        max(li.cluster_id) as cluster_id,
        max(li.cluster_alias) as cluster_alias,
        array_agg(DISTINCT li.namespace) as namespace,
        max(li.node) as node,
        max(li.resource_id) as resource_id,
        max(li.usage_date) as usage_start,
        max(li.usage_date) as usage_end,
        max(li.cost_entry_bill_id) as cost_entry_bill_id,
        max(li.subscription_guid) as subscription_guid,
        max(li.service_name) as service_name,
        max(li.instance_type) as instance_type,
        max(li.resource_location) as resource_location,
        max(li.currency) as currency,
        max(li.unit_of_measure) as unit_of_measure,
        li.tags,
        max(li.usage_quantity) as usage_quantity,
        max(li.pretax_cost) as pretax_cost,
        max(li.pretax_cost) * cast({{markup}} as decimal(24,9)) as markup_cost,
        max(li.shared_projects) as shared_projects,
        pc.project_costs as project_costs,
        '4963ab4d-a830-4226-ba3c-faeb9cf76f3e' -- '{{gcp_source_uuid | sqlsafe}}' as source_uuid
    FROM hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 as li
    JOIN cte_pod_project_cost as pc
        ON li.gcp_id = pc.gcp_id
    -- Dedup on gcp line item so we never double count usage or cost
    GROUP BY li.gcp_id, li.tags, pc.project_costs

    UNION

    SELECT max(li.report_period_id) as report_period_id,
        max(li.cluster_id) as cluster_id,
        max(li.cluster_alias) as cluster_alias,
        array_agg(DISTINCT li.namespace) as namespace,
        max(li.node) as node,
        max(li.resource_id) as resource_id,
        max(li.usage_date) as usage_start,
        max(li.usage_date) as usage_end,
        max(li.cost_entry_bill_id) as cost_entry_bill_id,
        max(li.subscription_guid) as subscription_guid,
        max(li.service_name) as service_name,
        max(li.instance_type) as instance_type,
        max(li.resource_location) as resource_location,
        max(li.currency) as currency,
        max(li.unit_of_measure) as unit_of_measure,
        li.tags,
        max(li.usage_quantity) as usage_quantity,
        max(li.pretax_cost) as pretax_cost,
        max(li.pretax_cost) * cast({{markup}} as decimal(24,9)) as markup_cost,
        max(li.shared_projects) as shared_projects,
        pc.project_costs as project_costs,
        '4963ab4d-a830-4226-ba3c-faeb9cf76f3e' -- '{{gcp_source_uuid | sqlsafe}}' as source_uuid
    FROM hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 AS li
    JOIN cte_storage_project_cost AS pc
        ON li.gcp_id = pc.gcp_id
    LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS ulid
        ON ulid.gcp_id = li.gcp_id
        AND ulid.gcp_id IS NULL
    GROUP BY li.gcp_id, li.tags, pc.project_costs
)
;

-- The full summary data for Openshift pod<->gcp and
-- Openshift volume<->gcp matches are UNIONed together
-- with a GROUP BY using the OCP ID to deduplicate
-- based on OpenShift data. This is effectively the same table
-- as reporting_ocpgcpcostlineitem_daily_summary but from the OpenShift
-- point of view. Here usage and cost are divided by the
-- number of pods sharing the cost so the values turn out the
-- same when reported.
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpcostlineitem_project_daily_summary_00000000_0000_0000_0000_000000000000;
CREATE TABLE hive.acct10001.__reporting_ocpgcpcostlineitem_project_daily_summary_00000000_0000_0000_0000_000000000000 AS (
    SELECT li.report_period_id,
        li.cluster_id,
        li.cluster_alias,
        'Pod' as data_source,
        li.namespace,
        li.node,
        li.pod_labels,
        max(li.resource_id) as resource_id,
        max(li.usage_date) as usage_start,
        max(li.usage_date) as usage_end,
        max(li.cost_entry_bill_id) as cost_entry_bill_id,
        max(li.subscription_guid) as subscription_guid,
        max(li.service_name) as service_name,
        max(li.instance_type) as instance_type,
        max(li.resource_location) as resource_location,
        max(li.currency) as currency,
        max(li.unit_of_measure) as unit_of_measure,
        li.tags,
        sum(li.usage_quantity / li.shared_projects) as usage_quantity,
        sum(li.pretax_cost / li.shared_projects) as pretax_cost,
        sum(li.pretax_cost / li.shared_projects) * cast({{markup}} as decimal(24,9)) as markup_cost,
        max(li.shared_projects) as shared_projects,
        li.project_cost,
        li.project_cost * cast({{markup}} as decimal(24,9)) as project_markup_cost,
        '4963ab4d-a830-4226-ba3c-faeb9cf76f3e' -- '{{gcp_source_uuid | sqlsafe}}' as source_uuid
    FROM hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 as li
    -- Grouping by OCP this time for the by project view
    GROUP BY li.report_period_id,
        li.ocp_id,
        li.cluster_id,
        li.cluster_alias,
        li.namespace,
        li.node,
        li.pod_labels,
        li.project_cost,
        li.tags

    UNION

    SELECT li.report_period_id,
        li.cluster_id,
        li.cluster_alias,
        'Storage' as data_source,
        li.namespace,
        li.node,
        li.volume_labels as pod_labels,
        max(li.resource_id) as resource_id,
        max(li.usage_date) as usage_start,
        max(li.usage_date) as usage_end,
        max(li.cost_entry_bill_id) as cost_entry_bill_id,
        max(li.subscription_guid) as subscription_guid,
        max(li.service_name) as service_name,
        max(li.instance_type) as instance_type,
        max(li.resource_location) as resource_location,
        max(li.currency) as currency,
        max(li.unit_of_measure) as unit_of_measure,
        li.tags,
        sum(li.usage_quantity / li.shared_projects) as usage_quantity,
        sum(li.pretax_cost / li.shared_projects) as pretax_cost,
        sum(li.pretax_cost / li.shared_projects) * cast({{markup}} as decimal(24,9)) as markup_cost,
        max(li.shared_projects) as shared_projects,
        li.project_cost,
        li.project_cost * cast({{markup}} as decimal(24,9)) as project_markup_cost,
        '4963ab4d-a830-4226-ba3c-faeb9cf76f3e' -- '{{gcp_source_uuid | sqlsafe}}' as source_uuid
    FROM hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000 AS li
    LEFT JOIN hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000 AS ulid
        ON ulid.gcp_id = li.gcp_id
    WHERE ulid.gcp_id IS NULL
    GROUP BY li.ocp_id,
        li.report_period_id,
        li.cluster_id,
        li.cluster_alias,
        li.namespace,
        li.node,
        li.volume_labels,
        li.project_cost,
        li.tags
)
;


-- Clear out old entries first
INSERT
  INTO postgres.acct10001.presto_delete_wrapper_log
       (
           id,
           action_ts,
           table_name,
           where_clause,
           result_rows
       )
VALUES (
    uuid(),
    now(),
    'reporting_ocpgcpcostlineitem_daily_summary',
    'WHERE usage_start >= '{{start_date}}'::date ' ||
      'AND usage_start <= '{{end_date}}'::date ' ||
      'AND cluster_id = ''my-ocp-cluster-gcp1' -- {{cluster_id}}' ' ||
      'AND cost_entry_bill_id = 3 -- {{bill_id}} ',
    null
)
;

-- Populate the daily aggregate line item data
INSERT INTO postgres.acct10001.reporting_ocpgcpcostlineitem_daily_summary (
    uuid,
    report_period_id,
    cluster_id,
    cluster_alias,
    namespace,
    node,
    resource_id,
    usage_start,
    usage_end,
    cost_entry_bill_id,
    subscription_guid,
    instance_type,
    service_name,
    resource_location,
    tags,
    usage_quantity,
    pretax_cost,
    markup_cost,
    currency,
    unit_of_measure,
    shared_projects,
    project_costs,
    source_uuid
)
    SELECT uuid(),
        report_period_id,
        cluster_id,
        cluster_alias,
        namespace,
        node,
        resource_id,
        usage_start,
        usage_end,
        cost_entry_bill_id,
        subscription_guid,
        instance_type,
        service_name,
        resource_location,
        json_parse(tags),
        cast(usage_quantity AS decimal(24,9)),
        cast(pretax_cost AS decimal(30,15)),
        cast(markup_cost  AS decimal(30,15)),
        currency,
        unit_of_measure,
        shared_projects,
        cast(project_costs AS JSON),
        cast(source_uuid AS UUID)
    FROM hive.acct10001.__reporting_ocpgcpcostlineitem_daily_summary_00000000_0000_0000_0000_000000000000
;


-- Clear out old entries first
INSERT
  INTO postgres.acct10001.presto_delete_wrapper_log
       (
           id,
           action_ts,
           table_name,
           where_clause,
           result_rows
       )
VALUES (
    uuid(),
    now(),
    'reporting_ocpgcpcostlineitem_project_daily_summary',
    'where usage_start >= '{{start_date}}'::date ' ||
      'and usage_start <= '{{end_date}}'::date ' ||
      'and cluster_id = ''my-ocp-cluster-gcp1' -- {{cluster_id}}' ' ||
      'and cost_entry_bill_id = 3 -- {{bill_id}} ',
    null
)
;

INSERT INTO postgres.acct10001.reporting_ocpgcpcostlineitem_project_daily_summary (
    uuid,
    report_period_id,
    cluster_id,
    cluster_alias,
    data_source,
    namespace,
    node,
    pod_labels,
    resource_id,
    usage_start,
    usage_end,
    cost_entry_bill_id,
    subscription_guid,
    instance_type,
    service_name,
    resource_location,
    usage_quantity,
    pretax_cost,
    markup_cost,
    currency,
    unit_of_measure,
    pod_cost,
    project_markup_cost,
    source_uuid
)
    SELECT uuid(),
        report_period_id,
        cluster_id,
        cluster_alias,
        data_source,
        namespace,
        node,
        json_parse(pod_labels),
        resource_id,
        usage_start,
        usage_end,
        cost_entry_bill_id,
        subscription_guid,
        instance_type,
        service_name,
        resource_location,
        cast(usage_quantity AS decimal(24,9)),
        cast(pretax_cost AS decimal(30,15)),
        cast(markup_cost AS decimal(30,15)),
        currency,
        unit_of_measure,
        cast(project_cost AS decimal(30,15)),
        cast(project_markup_cost AS decimal(30,15)),
        cast(source_uuid as UUID)
    FROM hive.acct10001.__reporting_ocpgcpcostlineitem_project_daily_summary_00000000_0000_0000_0000_000000000000
;


/*
 * ====================================
 *               CLEANUP
 * ====================================
 */
DELETE FROM hive.acct10001.__matched_tags_00000000_0000_0000_0000_000000000000;
DROP TABLE IF EXISTS hive.acct10001.__matched_tags_00000000_0000_0000_0000_000000000000;
DELETE FROM hive.acct10001.__reporting_ocpgcpcostlineitem_daily_summary_00000000_0000_0000_0000_000000000000;
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpcostlineitem_daily_summary_00000000_0000_0000_0000_000000000000;
DELETE FROM  hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000;
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpusagelineitem_daily_00000000_0000_0000_0000_000000000000;
DELETE FROM  hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000;
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpstoragelineitem_daily_00000000_0000_0000_0000_000000000000;
DELETE FROM hive.acct10001.__reporting_ocpgcpcostlineitem_project_daily_summary_00000000_0000_0000_0000_000000000000;
DROP TABLE IF EXISTS hive.acct10001.__reporting_ocpgcpcostlineitem_project_daily_summary_00000000_0000_0000_0000_000000000000;
