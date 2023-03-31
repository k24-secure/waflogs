WITH tmptable AS (
    SELECT httprequest.clientip AS "client_ip",
           httprequest.country AS "country",
           try((transform(filter(httprequest.headers, header -> LOWER(header.name) = 'user-agent'), header -> header.value))[1]) AS "user_agent",
           try(TRANSFORM(FILTER(labels, label -> label.name LIKE 'awswaf:managed:aws:bot-control:bot:name:%'), label -> SUBSTR(label.name, 41))[1]) AS "bot_name",
           try(TRANSFORM(FILTER(labels, label -> label.name LIKE 'awswaf:managed:aws:bot-control:bot:category:%'), label -> SUBSTR(label.name, 45))[1]) AS "bot_category",
           CARDINALITY(FILTER(labels, label -> label.name = 'awswaf:managed:aws:bot-control:signal:non_browser_user_agent')) > 0 AS "signal_nobrowser",
           terminatingruleid AS "terminating_rule",
           from_unixtime(FLOOR(timestamp/(1000*60*5))*60*5) as "time_window",
           timestamp
    FROM {{.WafTable}}
    WHERE day = '{{.Year}}/{{.Month}}/{{.Day}}'
)

SELECT {{.IdentityCols}},
       time_window,
       COUNT(*) AS "num_requests"
FROM tmptable
{{.CustomWhereClause}}
GROUP BY {{.IdentityCols}},
         time_window
HAVING COUNT(*) > {{.MinRate}}
ORDER BY num_requests DESC
LIMIT {{.Limit}};
