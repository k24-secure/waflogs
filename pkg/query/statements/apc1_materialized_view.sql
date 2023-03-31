CREATE TABLE IF NOT EXISTS waflog_{{.WAF}}_{{.Year}}_{{.Month}}_{{.Day}}
WITH (
      format = 'Parquet',
      write_compression = 'SNAPPY') AS

SELECT from_unixtime(timestamp/1000) as "timestamp",
       timestamp AS "unixtime",
       day AS "day",
       action AS "action",
       terminatingruleid AS "terminating_rule",
       httprequest.clientip AS "client_ip",
       httprequest.country AS "country",
       httprequest.httpmethod AS "method",
       httprequest.uri AS "uri",
       CONCAT(
         REGEXP_REPLACE(httprequest.uri, '^/ersatzteile-verschleissteile/.*', '/ersatzteile-verschleissteile/...'),
         '&',
         COALESCE(REGEXP_EXTRACT(httprequest.args, '^(rm=[a-zA-Z0-9]+)|^(rm=[a-zA-Z0-9]+)'), '')
         ) AS "uri_c",
       httprequest.args AS "params",
       TRY((TRANSFORM(FILTER(httprequest.headers, header -> LOWER(header.name) = 'user-agent'), header -> header.value))[1]) AS "user_agent",
       TRY((TRANSFORM(FILTER(httprequest.headers, header -> LOWER(header.name) = 'referer'), header -> header.value))[1]) AS "referer",
       TRY(TRANSFORM(FILTER(SPLIT(try((transform(filter(httprequest.headers, header -> LOWER(header.name) = 'cookie'), header -> header.value))[1]), ';'), kv -> SUBSTR(TRIM(LOWER(kv)), 1, 7) = 'session'), kv -> SPLIT(TRIM(kv), '=')[2])[1]) AS "c_session",
       CARDINALITY(FILTER(labels, label -> label.name = 'awswaf:managed:aws:bot-control:bot:verified')) > 0 AS "bot_verified",
       try(TRANSFORM(FILTER(labels, label -> label.name LIKE 'awswaf:managed:aws:bot-control:bot:category:%'), label -> SUBSTR(label.name, 45))[1]) AS "bot_category",
       try(TRANSFORM(FILTER(labels, label -> label.name LIKE 'awswaf:managed:aws:bot-control:bot:name:%'), label -> SUBSTR(label.name, 41))[1]) AS "bot_name",
       CARDINALITY(FILTER(labels, label -> label.name = 'awswaf:managed:aws:bot-control:signal:automated_browser')) > 0 AS "signal_automatedbrowser",
       CARDINALITY(FILTER(labels, label -> label.name = 'awswaf:managed:aws:bot-control:signal:non_browser_user_agent')) > 0 AS "signal_nobrowser",
       CARDINALITY(FILTER(labels, label -> label.name = 'awswaf:managed:aws:bot-control:signal:known_bot_data_center')) > 0 AS "signal_botdatacenter"
FROM {{.WafTable}}
WHERE day = '{{.Year}}/{{.Month}}/{{.Day}}'
