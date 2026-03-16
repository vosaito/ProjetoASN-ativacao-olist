-- Databricks notebook source
DROP TABLE IF EXISTS workspace.olist.fs_seller_vendedor;

CREATE TABLE workspace.olist.fs_seller_vendedor

WITH tb_base AS (
    SELECT v.idVendedor,
          p.dtPedido,
          p.idPedido,
          v.descUF AS ufVendedor,
          CASE
            WHEN v.descUF IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
            WHEN v.descUF IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
            WHEN v.descUF IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
            WHEN v.descUF IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
            WHEN v.descUF IN ('PR', 'RS', 'SC') THEN 'Sul'
          END AS regUFVendedor,
          c.idClienteUnico,
          c.descUF AS ufCliente,
          ip.vlPreco + ip.vlFrete AS vlPagamento
    FROM workspace.olist.pedido AS p

    LEFT JOIN workspace.olist.item_pedido AS ip
    ON p.idPedido = ip.idPedido

    LEFT JOIN workspace.olist.vendedor AS v
    ON ip.idVendedor = v.idVendedor

    LEFT JOIN workspace.olist.cliente AS c
    ON p.idCliente = c.idCliente

    WHERE date(dtPedido) < '2017-06-01'
    AND v.idVendedor IS NOT NULL
),
tb_gmvUF AS (
    SELECT ufCliente,
          SUM(vlpagamento) AS gmvUf
    FROM tb_base
    GROUP BY ufCliente
),
tb_join AS (
    SELECT '2017-06-01' AS dtRef, 
          t1.idVendedor,
          t1.ufVendedor,
          t1.regufvendedor,
          SUM(t1.vlPagamento) AS gmvTotal,
          SUM(CASE WHEN t1.ufCliente = 'SP' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasSP,
          SUM(CASE WHEN t1.ufCliente = 'SC' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasSC,
          SUM(CASE WHEN t1.ufCliente = 'MG' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasMG,
          SUM(CASE WHEN t1.ufCliente = 'PR' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasPR,
          SUM(CASE WHEN t1.ufCliente = 'RJ' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasRJ,
          SUM(CASE WHEN t1.ufCliente = 'RS' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasRS,
          SUM(CASE WHEN t1.ufCliente = 'PA' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasPA,
          SUM(CASE WHEN t1.ufCliente = 'GO' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasGO,
          SUM(CASE WHEN t1.ufCliente = 'ES' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasES,
          SUM(CASE WHEN t1.ufCliente = 'BA' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasBA,
          SUM(CASE WHEN t1.ufCliente = 'MA' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasMA,
          SUM(CASE WHEN t1.ufCliente = 'MS' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasMS,
          SUM(CASE WHEN t1.ufCliente = 'CE' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasCE,
          SUM(CASE WHEN t1.ufCliente = 'DF' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasDF,
          SUM(CASE WHEN t1.ufCliente = 'RN' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasRN,
          SUM(CASE WHEN t1.ufCliente = 'PE' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasPE,
          SUM(CASE WHEN t1.ufCliente = 'MT' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasMT,
          SUM(CASE WHEN t1.ufCliente = 'AM' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasAM,
          SUM(CASE WHEN t1.ufCliente = 'AP' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasAP,
          SUM(CASE WHEN t1.ufCliente = 'AL' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasAL,
          SUM(CASE WHEN t1.ufCliente = 'RO' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasRO,
          SUM(CASE WHEN t1.ufCliente = 'PB' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasPB,
          SUM(CASE WHEN t1.ufCliente = 'TO' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasTO,
          SUM(CASE WHEN t1.ufCliente = 'PI' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasPI,
          SUM(CASE WHEN t1.ufCliente = 'AC' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasAC,
          SUM(CASE WHEN t1.ufCliente = 'SE' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasSE,
          SUM(CASE WHEN t1.ufCliente = 'RR' THEN t1.vlPagamento ELSE 0 END) / SUM(t1.vlPagamento) AS pctGmvVendasRR,
          SUM(CASE WHEN t1.ufCliente = 'SP' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasSP,
          SUM(CASE WHEN t1.ufCliente = 'SC' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasSC,
          SUM(CASE WHEN t1.ufCliente = 'MG' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasMG,
          SUM(CASE WHEN t1.ufCliente = 'PR' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasPR,
          SUM(CASE WHEN t1.ufCliente = 'RJ' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasRJ,
          SUM(CASE WHEN t1.ufCliente = 'RS' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasRS,
          SUM(CASE WHEN t1.ufCliente = 'PA' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasPA,
          SUM(CASE WHEN t1.ufCliente = 'GO' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasGO,
          SUM(CASE WHEN t1.ufCliente = 'ES' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasES,
          SUM(CASE WHEN t1.ufCliente = 'BA' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasBA,
          SUM(CASE WHEN t1.ufCliente = 'MA' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasMA,
          SUM(CASE WHEN t1.ufCliente = 'MS' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasMS,
          SUM(CASE WHEN t1.ufCliente = 'CE' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasCE,
          SUM(CASE WHEN t1.ufCliente = 'DF' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasDF,
          SUM(CASE WHEN t1.ufCliente = 'RN' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasRN,
          SUM(CASE WHEN t1.ufCliente = 'PE' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasPE,
          SUM(CASE WHEN t1.ufCliente = 'MT' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasMT,
          SUM(CASE WHEN t1.ufCliente = 'AM' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasAM,
          SUM(CASE WHEN t1.ufCliente = 'AP' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasAP,
          SUM(CASE WHEN t1.ufCliente = 'AL' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasAL,
          SUM(CASE WHEN t1.ufCliente = 'RO' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasRO,
          SUM(CASE WHEN t1.ufCliente = 'PB' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasPB,
          SUM(CASE WHEN t1.ufCliente = 'TO' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasTO,
          SUM(CASE WHEN t1.ufCliente = 'PI' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasPI,
          SUM(CASE WHEN t1.ufCliente = 'AC' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasAC,
          SUM(CASE WHEN t1.ufCliente = 'SE' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasSE,
          SUM(CASE WHEN t1.ufCliente = 'RR' THEN t1.vlPagamento ELSE 0 END) / SUM(t2.gmvUf) AS shareGmvVendasRR
    FROM tb_base AS t1
    LEFT JOIN tb_gmvUF AS t2
    GROUP BY ALL
    ORDER BY t1.idVendedor
)

SELECT *
FROM tb_join 

-- COMMAND ----------


