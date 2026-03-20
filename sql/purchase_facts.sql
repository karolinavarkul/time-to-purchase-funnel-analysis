-- =====================================================
-- PURCHASE FACTS 
-- Project: Time to Purchase Funnel Analysis
-- =====================================================

-- Create campaign list:
WITH campaign_list AS (
  SELECT 'NewYear_V1' AS campaign_name UNION ALL
  SELECT 'NewYear_V2' UNION ALL
  SELECT 'BlackFriday_V1' UNION ALL
  SELECT 'BlackFriday_V2' UNION ALL
  SELECT 'Holiday_V1' UNION ALL
  SELECT 'Holiday_V2' UNION ALL
  SELECT 'Data Share Promo'
),
-- filter needed columns from raw_events:
base_events AS (
  SELECT
    user_pseudo_id,
    event_timestamp AS event_ts,
    event_date,
    event_name,
    campaign,
    category AS device,
    language,
    browser,
    country,
    purchase_revenue_in_usd AS purchase_revenue
  FROM `portfolio_product_analytics.raw_events`
),
-- sessionize events:
events_sessionized AS (
  SELECT
    *,
    CASE
      WHEN LAG(event_ts) OVER (
        PARTITION BY user_pseudo_id
        ORDER BY event_ts
      ) IS NULL THEN 1
      WHEN event_ts - LAG(event_ts) OVER (
        PARTITION BY user_pseudo_id
        ORDER BY event_ts
      ) > 1800000000 THEN 1   -- 30 minutes in microseconds
      ELSE 0
    END AS is_new_session
  FROM base_events
),
-- number sessions by user:
sessions_numbered AS (
  SELECT
    *,
    SUM(is_new_session) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_n,
    CONCAT(user_pseudo_id, "_", SUM(is_new_session) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) AS session_id
  FROM events_sessionized
),
-- session level aggregations:
session_aggregates AS (
  SELECT
    session_id,
    user_pseudo_id,
    session_n,

    DATE(TIMESTAMP_MICROS(MIN(event_ts))) AS session_date,
    MIN(event_ts) AS session_start_ts,
    MAX(event_ts) AS session_end_ts,
    MAX(event_ts) - MIN(event_ts) AS session_duration_micros,

    MAX(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS is_first_user_visit,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS made_purchase,
    COUNTIF(event_name = 'purchase') AS purchases_in_session,
    SUM(COALESCE(purchase_revenue, 0)) AS session_revenue,
    
    ARRAY_AGG(device IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS device,
    ARRAY_AGG(browser IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS browser,
    ARRAY_AGG(language IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS language,
    ARRAY_AGG(country IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS country,
    
    ARRAY_AGG(
      IF(campaign IN (SELECT campaign_name FROM campaign_list), campaign, NULL)
      IGNORE NULLS
      ORDER BY event_ts
      LIMIT 1
    )[SAFE_OFFSET(0)] AS session_campaign,

    COALESCE(
      CASE
        WHEN ARRAY_AGG(
          IF(campaign IN (SELECT campaign_name FROM campaign_list), campaign, NULL)
          IGNORE NULLS
          ORDER BY event_ts
          LIMIT 1
        )[SAFE_OFFSET(0)] IS NOT NULL THEN 'Campaign'
      END,
      ARRAY_AGG(
        CASE
          WHEN campaign = '(direct)' THEN 'Direct'
          WHEN campaign = '(organic)' THEN 'Organic'
          WHEN campaign = '(referral)' THEN 'Referral'
          WHEN campaign IN ('<Other>', '(data deleted)') OR campaign IS NULL THEN 'Other / Unknown'
          ELSE 'Other / Unknown'
        END
        IGNORE NULLS
        ORDER BY event_ts
        LIMIT 1
      )[SAFE_OFFSET(0)]
    ) AS session_channel,

    CASE
      WHEN (MAX(event_ts) - MIN(event_ts)) >= 10000000 THEN 0  -- 10 seconds
      WHEN MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) = 1 THEN 0
      WHEN COUNTIF(event_name = 'page_view') >= 2 THEN 0
      ELSE 1
    END AS is_bounced_session
  FROM sessions_numbered
  GROUP BY
    session_id,
    user_pseudo_id,
    session_n
),
-- extract purchase information:
purchases AS (
  SELECT
    sn.session_id,
    sn.user_pseudo_id,
    ROW_NUMBER() OVER (PARTITION BY sn.user_pseudo_id ORDER BY sn.event_ts) AS user_purchase_n,
    sn.session_n,
    TIMESTAMP_MICROS(sa.session_start_ts) AS session_start_ts,
    sa.session_date,
    sa.device,
    sa.browser,
    sa.language,
    sa.country,
    sa.session_channel,
    sa.session_campaign,
    sa.purchases_in_session,
    TIMESTAMP_MICROS(sn.event_ts) AS purchase_ts,
    sn.purchase_revenue,
    (sn.event_ts - sa.session_start_ts)/60000000.0 AS time_to_purchase_min,
    ROW_NUMBER() OVER (PARTITION BY sa.session_id ORDER BY sn.event_ts) AS purchase_n_in_session
  FROM sessions_numbered AS sn
  LEFT JOIN session_aggregates AS sa 
  ON sn.session_id = sa.session_id
  WHERE sn.event_name = "purchase"
)
SELECT
  *
FROM purchases
WHERE purchase_n_in_session=1