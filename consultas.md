¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones) durante el periodo
cubierto por los datos?

SELECT
    MIN(invoice_date) AS fecha_minima,
    MAX(invoice_date) AS fecha_maxima
FROM ecommerce_transacciones;

SELECT
    DATE_TRUNC('month', invoice_date) AS mes,
    SUM(total_venta) AS ventas_totales
FROM ecommerce_transacciones
GROUP BY mes
ORDER BY mes;


--------------------------------------------------------------------------------------------------------------------------

¿Qué categorías de producto generaron más revenue bruto?



SELECT
    segmento_venta ,
    ROUND(SUM(total_venta), 2) AS revenue_total
FROM ecommerce_transacciones
GROUP BY segmento_venta 
ORDER BY revenue_total DESC;


cuáles tuvieron mayor proporción de
devoluciones?

SELECT
    p.segmento_venta,
    SUM(t.quantity::integer) AS total_unidades_devueltas
FROM ecommerce_transacciones_raw t
INNER JOIN ecommerce_transacciones p
    ON t.id = p.id
WHERE t.quantity::integer < 0
GROUP BY p.segmento_venta 
ORDER BY total_unidades_devueltas ASC;


----------------------------------------------------------------------------------------------------------------------------

¿Cuáles son los 10 productos con mayor revenue neto?

SELECT
    id,
    SUM(quantity) AS total_vendido
FROM ecommerce_transacciones et 
WHERE quantity > 0
GROUP BY id
ORDER BY total_vendido desc
limit 10;


cuáles son los 10 con mayor tasa de
devolución?

SELECT
    p.id,
    SUM(t.quantity::integer) AS total_unidades_devueltas
FROM ecommerce_transacciones_raw t
INNER JOIN ecommerce_transacciones p
    ON t.id = p.id
WHERE t.quantity::integer < 0
GROUP BY p.id
ORDER BY total_unidades_devueltas asc
limit 10;


-----------------------------------------------------------------------------------------------------------------------------


¿Qué países concentran la mayor parte de las transacciones y cómo varía el ticket promedio entre ellos?

SELECT
    UPPER(TRIM(country)) AS pais,
    COUNT(*) AS cantidad_ventas,
    ROUND(SUM(total_venta), 2) AS ventas_totales,
    ROUND(SUM(total_venta) / COUNT(*), 2) AS ticket_promedio
FROM ecommerce_transacciones
GROUP BY UPPER(TRIM(country))
ORDER BY ventas_totales desc
limit 10;


----------------------------------------------------------------------------------------------------------------------------

¿Qué productos aparecen en las transacciones pero no tienen descripción consistente? 

SELECT *
FROM ecommerce_transacciones_raw etr 
WHERE etr.stock_code  ~ '^[A-Za-z]+$';





¿Cuántos
códigos únicos de producto existen en total?


cuales

SELECT DISTINCT stock_code
FROM ecommerce_transacciones;

cuantos

SELECT COUNT(DISTINCT stock_code) AS total_codigos_unicos
FROM ecommerce_transacciones;


--------------------------------------------------------------------------------------------------------------------------------


MI RECOMENDANCION: 

DATASETS
1) inicialmente eran 529.934, despues de la limpieza, quedaron 387.341, eso equivale a una perdida 29% de la informacion
es demasiado alta para tomar desiciones empreseriales, hay que buscrar estrategias para mejorar esta situacion. 

una puede ser unificar la manera en que se recolecta la informacion para que independiente del pais donde se esta tomando los datos, siempre mantengan la misma estructura y claridad de los datos para evitar perdidas.

A NIVEL DE NEGOCIO
2) el mercado de reino unido es el mas fuerte, pero tenemos unas ventas considerables en otros paises, por lo que si queremos hacer una expancion, lo mas recomendado seria hacerlo en alemania y francia donde se empiezan a concentrar ventas.