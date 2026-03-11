-- Databricks notebook source
-- JOINs das tabelas para gerar a base para avaliacao
WITH tb_base AS (
    SELECT DISTINCT v.idVendedor,
           p.dtPedido,
           p.idPedido,
           ap.vlNota
    FROM workspace.olist.pedido AS p

    LEFT JOIN workspace.olist.item_pedido AS ip
    ON p.idPedido = ip.idPedido

    LEFT JOIN workspace.olist.avaliacao_pedido AS ap
    ON ap.idPedido = p.idPedido

    LEFT JOIN workspace.olist.vendedor AS v
    ON v.idVendedor = ip.idVendedor

    WHERE date(dtPedido) < '2017-06-01'
),

tb_featAvaliacao AS (
    SELECT idVendedor,
          avg(vlNota) AS mediaAvaliacao,
          COUNT(vlNota) AS qtdeAvaliacoes,
          avg(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN vlNota END) AS mediaAvaliacao14d,
          COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN vlNota END) AS qtdeAvaliacoes14d,
          avg(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) AS mediaAvaliacao28d,
          COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) AS qtdeAvaliacoes28d,
          avg(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN vlNota END) AS mediaAvaliacao56d,
          COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN vlNota END) AS qtdeAvaliacoes56d,
          COUNT(CASE WHEN vlNota IS NULL THEN 1 END) / COUNT(idPedido) AS pctPedidoSemAvaliacao,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 1 THEN 1 END), COUNT(vlNota)) AS pctNota1,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 2 THEN 1 END), COUNT(vlNota)) AS pctNota2,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 3 THEN 1 END), COUNT(vlNota)) AS pctNota3,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 4 THEN 1 END), COUNT(vlNota)) AS pctNota4,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 5 THEN 1 END), COUNT(vlNota)) AS pctNota5,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 1 AND date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN vlNota END)) AS pctNota1d14,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 2 AND date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN vlNota END)) AS pctNota2d14,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 3 AND date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN vlNota END)) AS pctNota3d14,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 4 AND date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN vlNota END)) AS pctNota4d14,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 5 AND date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 14 DAY THEN vlNota END)) AS pctNota5d14,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 1 AND date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN vlNota END)) AS pctNota1d28,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 2 AND date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN vlNota END)) AS pctNota2d28,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 3 AND date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN vlNota END)) AS pctNota3d28,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 4 AND date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN vlNota END)) AS pctNota4d28,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 5 AND date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 28 DAY THEN vlNota END)) AS pctNota5d28,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 1 AND date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN vlNota END)) AS pctNota1d56,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 2 AND date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN vlNota END)) AS pctNota2d56,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 3 AND date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN vlNota END)) AS pctNota3d56,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 4 AND date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN vlNota END)) AS pctNota4d56,
          TRY_DIVIDE(COUNT(CASE WHEN vlNota = 5 AND date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN 1 END), COUNT(CASE WHEN date(dtPedido) > '2017-06-01' - INTERVAL 56 DAY THEN vlNota END)) AS pctNota5d56,
          AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 84 DAY THEN vlNota END) AS pctTendenciaNota1m3meses,
          CASE WHEN (AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 84 DAY THEN vlNota END)) > 1.05 THEN 'Crescente'
          WHEN (AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 84 DAY THEN vlNota END)) < 0.95 THEN 'Decrescente'
          ELSE 'Estavel' END AS descTendencia1m3meses,
          AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 168 DAY THEN vlNota END) AS pctTendenciaNota1m6meses,
          CASE WHEN (AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 168 DAY THEN vlNota END)) > 1.05 THEN 'Crescente'
          WHEN (AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 168 DAY THEN vlNota END)) < 0.95 THEN 'Decrescente'
          ELSE 'Estavel' END AS descTendencia1m6meses,
          AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 336 DAY THEN vlNota END) AS pctTendenciaNota1m12meses,
          CASE WHEN (AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 336 DAY THEN vlNota END)) > 1.05 THEN 'Crescente'
          WHEN (AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 28 DAY THEN vlNota END) / AVG(CASE WHEN date(dtPedido)> '2017-06-01' - INTERVAL 336 DAY THEN vlNota END)) < 0.95 THEN 'Decrescente'
          ELSE 'Estavel' END AS descTendencia1m12meses

    FROM tb_base

    GROUP BY idVendedor
)

SELECT '2017-06-01' AS dtRef,
        *
FROM tb_featAvaliacao

-- COMMAND ----------


