SELECT  c.categoria_mcc,
        COUNT(*)                                        AS total_transacciones,
        SUM(CASE WHEN t.es_fraude THEN 1 ELSE 0 END)    AS total_fraude,
        ROUND(100.0 * SUM(CASE WHEN t.es_fraude THEN 1 ELSE 0 END)
        / COUNT(*), 3)                                  AS tasa_fraude_pct
FROM    silver.transacciones t
JOIN    silver.comercios     c  ON t.id_comercio = c.id_comercio
WHERE   to_date(timestamp_seconds(timestamp_evento / 1000)) >= current_date() - INTERVAL 365 DAYS
GROUP BY c.categoria_mcc
ORDER BY tasa_fraude_pct DESC;