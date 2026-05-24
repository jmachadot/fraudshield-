SELECT  t.id_comercio,
        c.nombre_comercio,
        COUNT(*)        AS transacciones_fraude,
        SUM(t.monto)    AS monto_fraude_total
FROM    silver.transacciones t
JOIN    silver.comercios     c ON t.id_comercio = c.id_comercio
WHERE   t.es_fraude = TRUE
GROUP BY t.id_comercio, c.nombre_comercio
ORDER BY monto_fraude_total DESC
LIMIT 10;