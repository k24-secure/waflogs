WITH tmptable AS (
    SELECT httprequest.clientip AS "client_ip",
           httprequest.country AS "country",
           try((transform(filter(httprequest.headers, header -> LOWER(header.name) = 'user-agent'), header -> header.value))[1]) AS "user_agent",
           try(TRANSFORM(FILTER(labels, label -> label.name LIKE 'awswaf:managed:aws:bot-control:bot:name:%'), label -> SUBSTR(label.name, 41))[1]) AS "bot_name",
           try(TRANSFORM(FILTER(labels, label -> label.name LIKE 'awswaf:managed:aws:bot-control:bot:category:%'), label -> SUBSTR(label.name, 45))[1]) AS "bot_category",
           terminatingruleid AS "terminating_rule",
           timestamp
    FROM {{.WafTable}}
    WHERE day = '{{.Year}}/{{.Month}}/{{.Day}}'
      AND action = 'BLOCK'
)

SELECT {{.IdentityCols}},
       terminating_rule,
       COUNT(*) AS "num_requests"
FROM tmptable
WHERE terminating_rule IN (VALUES {{.TerminatingRules}})
GROUP BY {{.IdentityCols}},
         terminating_rule
ORDER BY num_requests DESC
LIMIT {{.Limit}};
