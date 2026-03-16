-- Databricks notebook source
DROP TABLE IF EXISTS workspace.olist.fs_seller_pagamento;

CREATE TABLE workspace.olist.fs_seller_pagamento

WITH tb_base AS (
    SELECT v.idVendedor,
          p.dtPedido,
          p.idPedido,
          p.idCliente,
          pp.descTipoPagamento,
          pp.nrParcelas,
          pp.vlPagamento

    FROM workspace.olist.pedido AS p

    LEFT JOIN workspace.olist.item_pedido AS ip
    ON p.idPedido = ip.idPedido

    LEFT JOIN workspace.olist.vendedor AS v
    ON ip.idVendedor = v.idVendedor

    LEFT JOIN workspace.olist.pagamento_pedido as pp
    ON p.idPedido = pp.idPedido

    WHERE date(dtPedido) < '2017-06-01'
    AND pp.descTipoPagamento IS NOT NULL
)

SELECT 
      idVendedor,
      SUM(vlPagamento) AS gmvGeral,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPagamento END), 0) AS gmvCC,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPagamento END), 0) AS gmvBoleto,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPagamento END), 0) AS gmvVoucher,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPagamento END), 0) AS gmvDebito,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPagamento END), 0) / SUM(vlPagamento) AS pctgmvCC,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPagamento END), 0) / SUM(vlPagamento) AS pctgmvBoleto,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPagamento END), 0) / SUM(vlPagamento) AS pctgmvVoucher,
      COALESCE(SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPagamento END), 0) / SUM(vlPagamento) AS pctgmvDebito,
      COUNT(CASE WHEN descTipoPagamento = 'credit_card' THEN idPedido END) / COUNT(idPedido) AS pctPedidoCC,
      COUNT(CASE WHEN descTipoPagamento = 'boleto' THEN idPedido END) / COUNT(idPedido) AS pctPedidoBoleto,
      COUNT(CASE WHEN descTipoPagamento = 'voucher' THEN idPedido END) / COUNT(idPedido) AS pctPedidoVoucher,
      COUNT(CASE WHEN descTipoPagamento = 'debit_card' THEN idPedido END) / COUNT(idPedido) AS pctPedidoDebito,
      AVG(CASE WHEN nrParcelas > 1 THEN nrParcelas END) AS mediaParcelas
FROM tb_base

GROUP BY idVendedor

-- COMMAND ----------

credit_card
boleto
voucher
debit_card
NULL
