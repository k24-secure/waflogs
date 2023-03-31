WITH waflog AS (

SELECT * FROM waflog_{{.WAF}}_{{.Year}}_{{.Month}}_{{.Day}}

), scraper_sessions AS (

SELECT DISTINCT c_session
       /*COUNT(*) AS "num_requests",
       CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(client_ip))) AS "ips",
       CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(user_agent))) AS "user_agents",
       CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(uri_c))) AS "uris",
       ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(day))) AS "days"*/
FROM waflog
WHERE c_session != ''
  AND terminating_rule NOT IN (VALUES 'waf-whitelist', 'bot-label-whitelist', 'seo-crawler', 'seo-crawler-vpn', 'allow-newrelic-header-check') /* ignore known bots */
  AND signal_nobrowser = False /*only user_agents that don't reveal themselves as bots*/
GROUP BY c_session
HAVING CARDINALITY(ARRAY_AGG(DISTINCT user_agent)) > 9

), scraped_urls AS (

SELECT uri_c,
       COUNT(*) AS "num_requests",
       ARRAY_SORT(ARRAY_AGG(DISTINCT day)) AS "days",
       CARDINALITY(ARRAY_AGG(DISTINCT user_agent)) AS "user_agents",
       CARDINALITY(ARRAY_AGG(DISTINCT uri)) AS "full_uris",
       CARDINALITY(ARRAY_AGG(DISTINCT waflog.c_session)) AS "sessions",
       HOUR(FROM_UNIXTIME(MIN(TO_UNIXTIME(timestamp)))) AS "first_hour",
       HOUR(FROM_UNIXTIME(MAX(TO_UNIXTIME(timestamp))))+1 AS "last_hour"
FROM waflog INNER JOIN scraper_sessions ON waflog.c_session = scraper_sessions.c_session
GROUP BY uri_c

)

SELECT *
FROM scraped_urls
ORDER BY num_requests DESC
LIMIT 100
