-- Databricks notebook source
DROP TABLE IF EXISTS workspace.olist.fs_seller_vendas;

CREATE TABLE workspace.olist.fs_seller_vendas

WITH tb_base AS (
    SELECT '2017-06-01' AS dtRef,
          v.idVendedor,
          p.idPedido,
          date(p.dtPedido) AS dtPedido,
          date(p.dtAprovado) AS dtAprovado,
          date(p.dtEntregue) AS dtEntregue,
          date(p.dtEstimativaEntrega) AS dtEstimativaEntrega,
          SUM(ip.vlPreco) AS vlPreco, 
          SUM(ip.vlFrete) AS vlFrete,
          COUNT(ip.idPedido) AS qtdeItens,
          pp.nrParcelas
    FROM workspace.olist.pedido AS p
    LEFT JOIN workspace.olist.item_pedido AS ip
    ON p.idPedido = ip.idPedido

    LEFT JOIN workspace.olist.vendedor AS v
    on ip.idVendedor = v.idVendedor

    LEFT JOIN workspace.olist.pagamento_pedido AS pp
    ON p.idPedido = pp.idPedido

    WHERE date(dtPedido) < '2017-06-01'
    AND ip.idVendedor IS NOT NULL
    AND pp.descTipoPagamento = 'credit_card'

    GROUP BY ALL
),
tb_featVendas AS (
    SELECT dtRef, 
          idVendedor,
          DATE_DIFF(dtRef, MAX(dtPedido)) AS RecenciaPedidoDias,
          DATE_DIFF(dtRef, MIN(dtPedido)) AS diasDoPrimeiroPedido,
          SUM(vlPreco) + SUM(vlFrete) / SUM(qtdeitens) AS ticketMedio,
          COUNT(DISTINCT idPedido) AS qtdePedidos,
          SUM(qtdeItens) AS qtdeItens,
          SUM(vlPreco) / SUM(qtdeitens) AS vlPrecoMedio,
          SUM(vlFrete) / SUM(qtdeitens) AS vlFreteMedio,
          CASE WHEN MAX(dtPedido) >= dtRef - INTERVAL 84 DAY THEN 1 ELSE 0 END AS flPedido3m,
          CASE WHEN MAX(dtPedido) >= dtRef - INTERVAL 168 DAY THEN 1 ELSE 0 END AS flPedido6m,
          CASE WHEN MAX(dtPedido) >= dtRef - INTERVAL 252 DAY THEN 1 ELSE 0 END AS flPedido9m,
          CASE WHEN MAX(dtPedido) >= dtRef - INTERVAL 336 DAY THEN 1 ELSE 0 END AS flPedido12m,
          DATE_DIFF(MAX(dtPedido), MIN(dtPedido)) AS intervaloPrimeiroUltimoPedido,
          COUNT(dtAprovado) / COUNT(dtPedido) AS pctPedidoAprovado,
          AVG(DATE_DIFF(dtEstimativaEntrega, dtPedido)) AS mediaPrazoEntrega,
          SUM(vlFrete)/SUM(vlPreco) AS pctFretePorPreco,
          SUM(vlFrete) AS vlFreteTotal,
          COUNT(CASE WHEN nrParcelas > 1 THEN 1 END) / COUNT(DISTINCT idPedido) AS pctPedidoParcelado,
          COUNT(CASE WHEN dtEntregue <= dtEstimativaEntrega THEN 1 END) / COUNT(DISTINCT idPedido) AS pctPedidoNoPrazo,
          TRY_DIVIDE(COUNT(CASE WHEN dtPedido >= dtRef - INTERVAL 28 DAY THEN idPedido END), COUNT(CASE WHEN dtPedido >= dtRef - INTERVAL 56 DAY AND dtPedido < dtRef - INTERVAL 28 DAY THEN idPedido END)) AS txCrescimentoM1,
          TRY_DIVIDE(COUNT(CASE WHEN dtPedido >= dtRef - INTERVAL 56 DAY AND dtPedido < dtRef - INTERVAL 28 DAY THEN idPedido END), COUNT(CASE WHEN dtPedido >= dtRef - INTERVAL 84 DAY AND dtPedido < dtRef - INTERVAL 56 DAY THEN idPedido END)) AS txCrescimentoM2,
          TRY_DIVIDE(COUNT(CASE WHEN dtPedido >= dtRef - INTERVAL 84 DAY AND dtPedido < dtRef - INTERVAL 56 DAY THEN idPedido END), COUNT(CASE WHEN dtPedido >= dtRef - INTERVAL 112 DAY AND dtPedido < dtRef - INTERVAL 84 DAY THEN idPedido END)) AS txCrescimentoM3,
          COUNT(CASE WHEN dtPedido >= dtRef - INTERVAL 84 DAY THEN idPedido END) / 3 AS mediaPedidosM3
    FROM tb_base

    GROUP BY ALL
),
tb_lagdt AS (
  SELECT idVendedor,
        dtPedido, 
        lag(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS dtPedidoAnterior
  FROM tb_base
),

tb_featLag AS (
  SELECT idVendedor,
          AVG(DATE_DIFF(dtPedido, dtPedidoAnterior)) AS mediaDiasEntrePedidos
  FROM tb_lagdt
  GROUP BY idVendedor
),

tb_weekly AS (
    SELECT idVendedor,
          YEAR(dtPedido) || weekofyear(dtPedido) AS yearweek,
          COUNT(idPedido) AS qtdePedido
    FROM tb_base
    GROUP BY ALL
),
-- Considerando apenas as semanas que o Vendedor vendeu.
tb_featweekly AS (
    SELECT idVendedor,
          stddev_pop(qtdePedido) AS stdPedidoSemanal
    FROM tb_weekly
    GROUP BY idVendedor
),

tb_VendaMes AS (
      SELECT idVendedor,
            dtRef,
            DATE(DATE_TRUNC('MONTH', dtPedido)) AS dtAnoMes,
            COUNT(DISTINCT idPedido) AS qtdePedidosMes
      FROM tb_base
      WHERE dtPedido >= date(dtRef) - INTERVAL 12 MONTH
      GROUP BY ALL
),

tb_pivotFlVendaMes AS (
      SELECT idVendedor,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 1 MONTH THEN 1 END) AS flVendaM1,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 2 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 1 MONTH THEN 1 END) AS flVendaM2,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 3 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 2 MONTH THEN 1 END) AS flVendaM3,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 4 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 3 MONTH THEN 1 END) AS flVendaM4,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 5 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 4 MONTH THEN 1 END) AS flVendaM5,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 6 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 5 MONTH THEN 1 END) AS flVendaM6,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 7 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 6 MONTH THEN 1 END) AS flVendaM7,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 8 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 7 MONTH THEN 1 END) AS flVendaM8,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 9 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 8 MONTH THEN 1 END) AS flVendaM9,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 10 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 9 MONTH THEN 1 END) AS flVendaM10,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 11 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 10 MONTH THEN 1 END) AS flVendaM11,
            COUNT(CASE WHEN dtAnoMes >= date(dtRef) - INTERVAL 12 MONTH AND dtAnoMes < date(dtRef) - INTERVAL 11 MONTH THEN 1 END) AS flVendaM12
      FROM tb_VendaMes
      GROUP BY ALL
),

tb_featVenda12m AS (
      SELECT idVendedor,
            12 - (flVendaM1 + flVendaM2 + flVendaM3 + flVendaM4 + flVendaM5 + flVendaM6 + flVendaM7 + flVendaM8 + flVendaM9 + flVendaM10 + flVendaM11 + flVendaM12) AS sumVendas12m
      FROM tb_pivotFlVendaMes

),

tb_join AS (
  SELECT *
  FROM tb_featVendas 
  LEFT JOIN tb_featLag USING (idVendedor)
  LEFT JOIN tb_featweekly USING (idVendedor)
  LEFT JOIN tb_pivotFlVendaMes USING (idVendedor)
  LEFT JOIN tb_featVenda12m USING (idVendedor)
  ORDER BY tb_featVendas.idVendedor
)

SELECT *
FROM tb_join

-- COMMAND ----------


