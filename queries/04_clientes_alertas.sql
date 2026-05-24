SELECT  a.id_cliente,
        COUNT(*)                AS total_alertas,
        AVG(a.probabilidad)     AS riesgo_promedio,
        MAX(a.probabilidad)     AS riesgo_maximo
FROM    gold.alertas a
WHERE   a.fecha_alerta >= current_date() - INTERVAL 7 DAYS
GROUP BY a.id_cliente
HAVING  COUNT(*) >= 3
ORDER BY total_alertas DESC, riesgo_promedio DESC;