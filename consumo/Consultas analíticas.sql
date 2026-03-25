-- 1. Churn por mes y suscripcion
SELECT
  date_format(fecha_churn, 'y-MM') as mes_churn, -- https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
  s.nombre_suscripcion,
  COUNT(*) AS cantidad_churn
FROM oro_dev.analisis_suscripciones_bruno.fact_suscripciones_cuentas fsc
JOIN oro_dev.dimensiones_comunes_bruno.dim_cuentas c
  ON fsc.id_dim_cuenta = c.id_dim_cuenta
JOIN oro_dev.dimensiones_comunes_bruno.dim_suscripciones s
  ON fsc.id_dim_suscripcion = s.id_dim_suscripcion
WHERE fecha_churn is not null
GROUP BY mes_churn, s.nombre_suscripcion
ORDER BY mes_churn desc, s.nombre_suscripcion;

-- 2. Consulta para fact_funcionalidades_premium_cuentas cruzando con cuenta y funcionalidad
SELECT
  c.id_cuenta,
  c.nombre_cuenta,
  COUNT(*) AS total_compras,
  SUM(fpc.monto_pagado) AS monto_total_pagado
FROM oro_dev.analisis_suscripciones_bruno.fact_funcionalidades_premium_cuentas fpc
JOIN oro_dev.dimensiones_comunes_bruno.dim_cuentas c
  ON fpc.id_dim_cuenta = c.id_dim_cuenta
JOIN oro_dev.dimensiones_comunes_bruno.dim_funcionalidades_premium fp
  ON fpc.id_dim_funcionalidad = fp.id_dim_funcionalidad
GROUP BY c.id_cuenta, c.nombre_cuenta
order by total_compras desc;

-- 3. Drill across: Métricas de ambas facts para una misma cuenta.
-- Una de las ventajas del modelo dimensional es que podemos usar atributos de dimensiones conformadas para cruzar métricas de distintos procesos de negocio.
-- Pero siempre hay que calcular por tabla de hecho y después cruzar (drill-across), nunca joinear directo las tablas de hechos porque eso puede generar números incorrectos si el grano de las tablas de hecho no coinciden.
WITH fsc AS (
  SELECT
    dc.id_cuenta,
    fs.fecha_inicio AS fecha_upgrade
  FROM oro_dev.analisis_suscripciones_bruno.fact_suscripciones_cuentas fs
  JOIN oro_dev.dimensiones_comunes_bruno.dim_cuentas dc
    ON fs.id_dim_cuenta = dc.id_dim_cuenta
  WHERE fs.tipo_cambio = 'Upgrade'
),
fpc AS (
  SELECT
    dc.id_cuenta,
    fp.fecha_compra,
    COUNT(fp.id_dim_funcionalidad) AS compras_anteriores,
    SUM(fp.monto_pagado) AS monto_total_anteriores
  FROM oro_dev.analisis_suscripciones_bruno.fact_funcionalidades_premium_cuentas fp
  JOIN oro_dev.dimensiones_comunes_bruno.dim_cuentas dc
    ON fp.id_dim_cuenta = dc.id_dim_cuenta
  GROUP BY dc.id_cuenta, fp.fecha_compra
)
SELECT
  fsc.id_cuenta,
  fsc.fecha_upgrade,
  COALESCE(SUM(fpc.compras_anteriores), 0) AS compras_anteriores,
  COALESCE(SUM(fpc.monto_total_anteriores), 0) AS monto_total_anteriores
FROM fsc
LEFT JOIN fpc
  ON fsc.id_cuenta = fpc.id_cuenta
  AND fpc.fecha_compra < fsc.fecha_upgrade
GROUP BY fsc.id_cuenta, fsc.fecha_upgrade
