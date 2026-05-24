SELECT  fecha,
        verdaderos_positivos,
        falsos_positivos,
        falsos_negativos,
        ROUND(100.0 * verdaderos_positivos
        / (verdaderos_positivos + falsos_negativos), 2) AS recall_pct,
        ROUND(100.0 * verdaderos_positivos
        / (verdaderos_positivos + falsos_positivos), 2) AS precision_pct
FROM    gold.indicadores_modelo
ORDER BY fecha DESC;