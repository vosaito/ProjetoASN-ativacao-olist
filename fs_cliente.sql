-- Databricks notebook source
WITH tb_base AS (
    SELECT v.idVendedor,
          p.dtPedido,
          c.idCliente,
          c.idClienteUnico,
          c.descUF AS ufCliente,
          p.idPedido
    FROM workspace.olist.pedido AS p

    LEFT JOIN workspace.olist.item_pedido AS ip
    ON p.idPedido = ip.idPedido

    LEFT JOIN workspace.olist.cliente AS c
    ON p.idCliente = c.idCliente

    LEFT JOIN workspace.olist.vendedor AS v
    ON v.idVendedor = ip.idVendedor

    WHERE p.dtPedido < '2017-06-01'
    AND v.idVendedor IS NOT NULL
),

tb_vendasAcumCliente AS (
    SELECT idVendedor,
          ufCliente,
          date(dtPedido) AS dtdiaPedido,
          idClienteUnico,
          COUNT(idPedido) AS qtdePedido,
          CASE WHEN dtDiaPedido > '2017-06-01' - INTERVAL 28 DAY THEN 1 ELSE 0 END AS compraNoMes,
          CASE WHEN dtDiaPedido < '2017-06-01' - INTERVAL 28 DAY THEN 1 ELSE 0 END AS compraMesesAnteriores    FROM tb_base

    GROUP BY idVendedor, ufCliente, dtdiaPedido, idClienteUnico
    ORDER BY idVendedor
),

tb_vendasConsolidada AS (
    SELECT idVendedor,
          SUM(qtdePedido) AS qtdePedidos,
          COUNT(idClienteUnico) AS qtdeClientes,
          SUM(CASE WHEN compraNoMes = 1 AND compraMesesAnteriores = 0 THEN 1 ELSE 0 END) AS qtdeClienteNovo,
          SUM(CASE WHEN compraNoMes = 1 AND compraMesesAnteriores = 1 THEN 1 ELSE 0 END) AS qtdeClienteRecorrente,
          SUM(CASE WHEN compraNoMes = 0 AND compraMesesAnteriores = 1 THEN 1 ELSE 0 END) AS qtdeClientePontual,
          SUM(CASE WHEN compraNoMes = 1 AND compraMesesAnteriores = 0 THEN 1 ELSE 0 END) / COUNT(idClienteUnico) AS pctClienteNovo,
          SUM(CASE WHEN compraNoMes = 1 AND compraMesesAnteriores = 1 THEN 1 ELSE 0 END) / COUNT(idClienteUnico) AS pctClienteRecorrente,
          SUM(CASE WHEN compraNoMes = 0 AND compraMesesAnteriores = 1 THEN 1 ELSE 0 END) / COUNT(idClienteUnico) AS pctClientePontual,
          COUNT(CASE WHEN ufCLiente = 'AM' THEN idClienteUnico END) AS qtdeCLienteAM,
          COUNT(CASE WHEN ufCLiente = 'TO' THEN idClienteUnico END) AS qtdeCLienteTO,
          COUNT(CASE WHEN ufCLiente = 'MA' THEN idClienteUnico END) AS qtdeCLienteMA,
          COUNT(CASE WHEN ufCLiente = 'PA' THEN idClienteUnico END) AS qtdeCLientePA,
          COUNT(CASE WHEN ufCLiente = 'SP' THEN idClienteUnico END) AS qtdeCLienteSP,
          COUNT(CASE WHEN ufCLiente = 'ES' THEN idClienteUnico END) AS qtdeCLienteES,
          COUNT(CASE WHEN ufCLiente = 'PI' THEN idClienteUnico END) AS qtdeCLientePI,
          COUNT(CASE WHEN ufCLiente = 'AC' THEN idClienteUnico END) AS qtdeCLienteAC,
          COUNT(CASE WHEN ufCLiente = 'BA' THEN idClienteUnico END) AS qtdeCLienteBA,
          COUNT(CASE WHEN ufCLiente = 'RS' THEN idClienteUnico END) AS qtdeCLienteRS,
          COUNT(CASE WHEN ufCLiente = 'GO' THEN idClienteUnico END) AS qtdeCLienteGO,
          COUNT(CASE WHEN ufCLiente = 'AP' THEN idClienteUnico END) AS qtdeCLienteAP,
          COUNT(CASE WHEN ufCLiente = 'MG' THEN idClienteUnico END) AS qtdeCLienteMG,
          COUNT(CASE WHEN ufCLiente = 'RO' THEN idClienteUnico END) AS qtdeCLienteRO,
          COUNT(CASE WHEN ufCLiente = 'PR' THEN idClienteUnico END) AS qtdeCLientePR,
          COUNT(CASE WHEN ufCLiente = 'SC' THEN idClienteUnico END) AS qtdeCLienteSC,
          COUNT(CASE WHEN ufCLiente = 'RJ' THEN idClienteUnico END) AS qtdeCLienteRJ,
          COUNT(CASE WHEN ufCLiente = 'SE' THEN idClienteUnico END) AS qtdeCLienteSE,
          COUNT(CASE WHEN ufCLiente = 'DF' THEN idClienteUnico END) AS qtdeCLienteDF,
          COUNT(CASE WHEN ufCLiente = 'PB' THEN idClienteUnico END) AS qtdeCLientePB,
          COUNT(CASE WHEN ufCLiente = 'MS' THEN idClienteUnico END) AS qtdeCLienteMS,
          COUNT(CASE WHEN ufCLiente = 'PE' THEN idClienteUnico END) AS qtdeCLientePE,
          COUNT(CASE WHEN ufCLiente = 'RN' THEN idClienteUnico END) AS qtdeCLienteRN,
          COUNT(CASE WHEN ufCLiente = 'RR' THEN idClienteUnico END) AS qtdeCLienteRR,
          COUNT(CASE WHEN ufCLiente = 'MT' THEN idClienteUnico END) AS qtdeCLienteMT,
          COUNT(CASE WHEN ufCLiente = 'AL' THEN idClienteUnico END) AS qtdeCLienteAL,
          COUNT(CASE WHEN ufCLiente = 'CE' THEN idClienteUnico END) AS qtdeCLienteCE

    FROM tb_vendasAcumCliente

    GROUP BY idVendedor
    ORDER BY idVendedor
)

SELECT '2017-06-01' AS dtRer,
       * 
FROM tb_vendasConsolidada

-- COMMAND ----------


