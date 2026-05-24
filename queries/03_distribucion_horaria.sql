SELECT  HOUR(timestamp_seconds(timestamp_evento / 1000))   AS hora,
        SUM(CASE WHEN es_fraude THEN 0 ELSE 1 END)         AS legitimas,
        SUM(CASE WHEN es_fraude THEN 1 ELSE 0 END)         AS fraudulentas
FROM    silver.transacciones
WHERE   to_date(timestamp_seconds(timestamp_evento / 1000)) >= current_date() - INTERVAL 365 DAYS
GROUP BY HOUR(timestamp_seconds(timestamp_evento / 1000))
ORDER BY hora;