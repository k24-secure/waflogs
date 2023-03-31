WITH waflog AS (

SELECT * FROM waflog_{{.WAF}}_{{.Year}}_{{.Month}}_{{.Day}}

), scraper_sessions AS (

SELECT DISTINCT c_session
FROM waflog
WHERE c_session != ''
  AND terminating_rule NOT IN (VALUES 'waf-whitelist', 'bot-label-whitelist', 'seo-crawler', 'seo-crawler-vpn', 'allow-newrelic-header-check') /* ignore known bots */
  AND signal_nobrowser = False /*only user_agents that don't reveal themselves as bots*/
GROUP BY c_session
HAVING CARDINALITY(ARRAY_AGG(DISTINCT user_agent)) > 9

), scraper_user_agents AS (

SELECT DISTINCT user_agent, COUNT(*) AS "num_requests"
FROM waflog INNER JOIN scraper_sessions ON waflog.c_session = scraper_sessions.c_session
GROUP BY user_agent
HAVING COUNT(*) > 5000
)

SELECT DISTINCT regexp_extract(params, '.*[?&]?search=([^&]+).*', 1) AS "prodcut"
FROM waflog INNER JOIN scraper_sessions ON waflog.c_session = scraper_sessions.c_session
WHERE user_agent IN (SELECT user_agent FROM scraper_user_agents)
  AND uri = '/artikeldetails'
ORDER BY regexp_extract(params, '.*[?&]?search=([^&]+).*', 1) ASC
LIMIT {{.Limit}}
