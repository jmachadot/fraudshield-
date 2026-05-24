SELECT  id_cliente,
        id_transaccion,
        timestamp_evento,
        distancia_geo,
        velocidad_geo
FROM    gold.caracteristicas
WHERE   velocidad_geo > 900          -- km/h: umbral de plausibilidad
ORDER BY velocidad_geo DESC;