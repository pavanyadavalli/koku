-- ===================
-- START
-- ===================

SELECT namespace,
       SUM(
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'cpu')::numeric, 0) +
                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'memory')::numeric, 0)
                            ) +
                            COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'storage')::numeric,0)
                          ) +
                            COALESCE("reporting_ocpusagelineitem_daily_summary"."supplementary_monthly_cost",0)
                        ) +
                        COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_raw_cost", 0)
                      ) +
                      COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'cpu')::numeric,0)
                    ) +
                    COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'memory')::numeric, 0)
                  ) +
                  COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'storage')::numeric, 0)
                ) +
                COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_monthly_cost", 0)
              ) +
              COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_markup_cost",0)
            )
          ) AS "cost",
       row_number() OVER (ORDER BY SUM(
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'cpu')::numeric, 0) +
                                                          COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'memory')::numeric, 0)
                                                        ) +
                                                        COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'storage')::numeric,0)
                                                      ) +
                                                        COALESCE("reporting_ocpusagelineitem_daily_summary"."supplementary_monthly_cost",0)
                                                    ) +
                                                    COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_raw_cost", 0)
                                                  ) +
                                                  COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'cpu')::numeric,0)
                                                ) +
                                                COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'memory')::numeric, 0)
                                              ) +
                                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'storage')::numeric, 0)
                                            ) +
                                            COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_monthly_cost", 0)
                                          ) +
                                          COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_markup_cost",0)
                                        )
                                      )
                                   DESC) AS "rank"
  FROM "reporting_ocpusagelineitem_daily_summary"
 WHERE ("reporting_ocpusagelineitem_daily_summary"."usage_start" >= '2021-02-01'
   AND "reporting_ocpusagelineitem_daily_summary"."usage_end" <= '2021-02-24')
 GROUP BY namespace;



-- ===================
-- MOD 1
-- ===================

with ranked_cost_sum as (
  SELECT namespace,
         SUM(
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'cpu')::numeric, 0) +
                                COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'memory')::numeric, 0)
                              ) +
                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'storage')::numeric,0)
                            ) +
                              COALESCE("reporting_ocpusagelineitem_daily_summary"."supplementary_monthly_cost",0)
                          ) +
                          COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_raw_cost", 0)
                        ) +
                        COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'cpu')::numeric,0)
                      ) +
                      COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'memory')::numeric, 0)
                    ) +
                    COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'storage')::numeric, 0)
                  ) +
                  COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_monthly_cost", 0)
                ) +
                COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_markup_cost",0)
              )
            ) AS "cost",
         row_number() OVER (ORDER BY SUM(
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'cpu')::numeric, 0) +
                                                            COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'memory')::numeric, 0)
                                                          ) +
                                                          COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'storage')::numeric,0)
                                                        ) +
                                                          COALESCE("reporting_ocpusagelineitem_daily_summary"."supplementary_monthly_cost",0)
                                                      ) +
                                                      COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_raw_cost", 0)
                                                    ) +
                                                    COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'cpu')::numeric,0)
                                                  ) +
                                                  COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'memory')::numeric, 0)
                                                ) +
                                                COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'storage')::numeric, 0)
                                              ) +
                                              COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_monthly_cost", 0)
                                            ) +
                                            COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_markup_cost",0)
                                          )
                                        )
                                     DESC) AS "rank"
    FROM "reporting_ocpusagelineitem_daily_summary"
   WHERE ("reporting_ocpusagelineitem_daily_summary"."usage_start" >= '2021-02-01'
     AND "reporting_ocpusagelineitem_daily_summary"."usage_end" <= '2021-02-24')
   GROUP BY namespace
)
SELECT namespace,
       cost,
       rank,
       (SELECT MAX(rank) FROM ranked_cost_sum) as max_rank
  FROM ranked_cost_sum
 ORDER
    BY rank desc
 LIMIT 10
OFFSET 0;


cte = With(select_query, name="ranked_cost_sum")

print(sqlparse.format(str(cte.queryset().with_cte(cte).annotate(max_pk=RawSQL("""select max(source_id) from test_cte""", [])).query), reindent_aligned=True))
WITH RECURSIVE test_cte AS (
        SELECT "api_sources"."source_id",
               "api_sources"."source_uuid",
               "api_sources"."name",
               "api_sources"."auth_header",
               "api_sources"."offset",
               "api_sources"."account_id",
               "api_sources"."source_type",
               "api_sources"."authentication",
               "api_sources"."billing_source",
               "api_sources"."koku_uuid",
               "api_sources"."pending_delete",
               "api_sources"."pending_update",
               "api_sources"."out_of_order_delete",
               "api_sources"."status",
               ("api_sources"."source_id" % 2) AS "evenrow"
          FROM "api_sources"
         WHERE ("api_sources"."source_id" % 2) = 0
       ) SELECT "test_cte"."source_id",
       "test_cte"."source_uuid",
       "test_cte"."name",
       "test_cte"."auth_header",
       "test_cte"."offset",
       "test_cte"."account_id",
       "test_cte"."source_type",
       "test_cte"."authentication",
       "test_cte"."billing_source",
       "test_cte"."koku_uuid",
       "test_cte"."pending_delete",
       "test_cte"."pending_update",
       "test_cte"."out_of_order_delete",
       "test_cte"."status",
       "test_cte"."evenrow" AS "evenrow",
       (
        select max(source_id)
          from test_cte
       ) AS "max_pk"
  FROM "test_cte"
 ORDER BY "test_cte"."name" ASC



# Model changes
from django_cte import CTEManager
...
class MyModel(models.Model):
    objects = CTEManager()
    ...


# Business logic changes
from django_cte import With
from django.db.models.expressions import RawSQL
...
def get_ranked_project_cost_query(select_query, rank_col="_rank"):
    cte_name = "project_ranked_cost"
    max_rank_expr = RawSQL(f"""select max({rank_col}) from "{cte_name}" """, [])

    cte = With(select_query, name=cte_name)
    cte_query = cte.queryset().with_cte(cte).annotate(max_rank=max_rank_expr)

    return cte_query


# A silly example
# api/provider/models.py
from django_cte import CTEManager
...
class Sources(RunTextFieldValidators, models.Model):
    """Platform-Sources table.

    Used for managing Platform-Sources.
    """
    objects = CTEManager()
...

# python ./manage.py shell
from django_cte import With
from django.db.models import F
from django.db.models.expressions import RawSQL
from api.provider.models import Sources
from sqlparse import format as sqlformat
select_query = Sources.objects.annotate(evenrow=F('source_id') % 2).filter(evenrow=0)
cte_name = "project_ranked_cost"
rank_col = 'source_id'
max_rank_expr = RawSQL(f"""select max({rank_col}) from "{cte_name}" """, [])
cte = With(select_query, name=cte_name)
cte_query = cte.queryset().with_cte(cte).annotate(max_rank=max_rank_expr)
print(sqlformat(str(cte_query.query), reindent_aligned=True))
for rec in cte_query.values('source_id', 'max_rank':
    print(rec)














-- ===================
--   MOD 2
-- ===================


SELECT coalesce(namespace, 'No Project') as namespace,
       usage_start,
       SUM(
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'cpu')::numeric, 0) +
                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'memory')::numeric, 0)
                            ) +
                            COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'storage')::numeric,0)
                          ) +
                            COALESCE("reporting_ocpusagelineitem_daily_summary"."supplementary_monthly_cost",0)
                        ) +
                        COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_raw_cost", 0)
                      ) +
                      COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'cpu')::numeric,0)
                    ) +
                    COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'memory')::numeric, 0)
                  ) +
                  COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'storage')::numeric, 0)
                ) +
                COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_monthly_cost", 0)
              ) +
              COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_markup_cost",0)
            )
          ) AS "cost"
  FROM "reporting_ocpusagelineitem_daily_summary"
 WHERE ("reporting_ocpusagelineitem_daily_summary"."usage_start" >= '2021-02-01'
   AND "reporting_ocpusagelineitem_daily_summary"."usage_end" <= '2021-02-24')
 GROUP BY namespace, usage_start;


with daily_project_cost as (
SELECT coalesce(namespace, 'No Project') as namespace,
       usage_start,
       SUM(
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'cpu')::numeric, 0) +
                              COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'memory')::numeric, 0)
                            ) +
                            COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'storage')::numeric,0)
                          ) +
                            COALESCE("reporting_ocpusagelineitem_daily_summary"."supplementary_monthly_cost",0)
                        ) +
                        COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_raw_cost", 0)
                      ) +
                      COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'cpu')::numeric,0)
                    ) +
                    COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'memory')::numeric, 0)
                  ) +
                  COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'storage')::numeric, 0)
                ) +
                COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_monthly_cost", 0)
              ) +
              COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_markup_cost",0)
            )
          ) AS "cost"
  FROM "reporting_ocpusagelineitem_daily_summary"
 WHERE ("reporting_ocpusagelineitem_daily_summary"."usage_start" >= '2021-02-01'
   AND "reporting_ocpusagelineitem_daily_summary"."usage_end" <= '2021-02-24')
 GROUP BY namespace, usage_start
),
ranked_project as (
select namespace,
       row_number() over ( order by sum(cost) desc ) as _rank
  from daily_project_cost
 group by namespace
),
range_dates as (
select generate_series('2021-02-01'::timestamptz, '2021-02-24'::timestamptz, '1 day'::interval) as "range_day"
)
select case when rp._rank > 10 then 'Others' else rp.namespace end::text as "namespace",
       rd.range_day::date as "usage_start",
       sum(coalesce(dpc.cost, 0.0::numeric(25,15))) as cost,
       case when rp._rank > 10 then 99999 else rp._rank end::int as "rank",
       (select max(_rank) from ranked_project) as "max_rank"
  from ranked_project rp
 cross
  join range_dates rd
  left
  join daily_project_cost dpc
    on dpc.usage_start = rd.range_day
   and dpc.namespace = rp.namespace
 group
    by case when rp._rank > 10 then 99999 else rp._rank end::int,
       case when rp._rank > 10 then 'Others' else rp.namespace end::text,
       rd.range_day::date
 order
    by rd.range_day::date,
       case when rp._rank > 10 then 99999 else rp._rank end::int



with daily_project_cost as (
SELECT namespace,
       usage_start,
       SUM((((((((((COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'cpu')::numeric, 0) +
                    COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'memory')::numeric, 0)) +
                   COALESCE(("reporting_ocpusagelineitem_daily_summary"."supplementary_usage_cost" ->> 'storage')::numeric,0)) +
                  COALESCE("reporting_ocpusagelineitem_daily_summary"."supplementary_monthly_cost",0)) +
                 COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_raw_cost", 0)) +
                COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'cpu')::numeric,0)) +
               COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'memory')::numeric, 0)) +
              COALESCE(("reporting_ocpusagelineitem_daily_summary"."infrastructure_usage_cost" ->> 'storage')::numeric, 0)) +
             COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_monthly_cost", 0)) +
            COALESCE("reporting_ocpusagelineitem_daily_summary"."infrastructure_project_markup_cost",0))) AS "cost"
  FROM "reporting_ocpusagelineitem_daily_summary"
 WHERE ("reporting_ocpusagelineitem_daily_summary"."usage_start" >= '2021-02-01'
   AND "reporting_ocpusagelineitem_daily_summary"."usage_end" <= '2021-02-24')
 GROUP BY namespace, usage_start
),
ranked_project as (
select namespace,
       row_number() over ( order by sum(cost) desc ) as _rank
  from daily_project_cost
 group by namespace
),
range_dates as (
select generate_series('2021-02-01'::timestamptz, '2021-02-24'::timestamptz, '1 day'::interval) as "range_day"
)
select case when rp._rank > 10 then 'Others' else rp.namespace end::text as "namespace",
       rd.range_day::date as "usage_start",
       sum(coalesce(dpc.cost, 0.0::numeric(25,15))) as cost,
       case when rp._rank > 10 then -1 else rp._rank end::int as "rank",
       sum((rp._rank > 10)::boolean::int) as "others_count"
  from ranked_project rp
 cross
  join range_dates rd
  left
  join daily_project_cost dpc
    on dpc.usage_start = rd.range_day
   and dpc.namespace = rp.namespace
 group
    by case when rp._rank > 10 then -1 else rp._rank end::int,
       case when rp._rank > 10 then 'Others' else rp.namespace end::text,
       rd.range_day::date
 order
    by rd.range_day::date,
       case when rp._rank > 10 then -1 else rp._rank end::int
