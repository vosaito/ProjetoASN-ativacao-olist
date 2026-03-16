-- Databricks notebook source
WITH tb_publico AS (
    SELECT DISTINCT t2.idVendedor

    FROM workspace.olist.pedido AS t1
    LEFT JOIN workspace.olist.item_pedido AS t2
    ON t1.idPedido = t2.idPedido

    WHERE date(t1.dtPedido) < '2017-06-01'
    AND t2.idVendedor IS NOT NULL
),
tb_publico_futuro AS (
    SELECT DISTINCT t2.idVendedor

    FROM workspace.olist.pedido AS t1
    LEFT JOIN workspace.olist.item_pedido AS t2
    ON t1.idPedido = t2.idPedido

    WHERE date(t1.dtPedido) < '2017-06-01' + INTERVAL 28 DAY
    AND date(t1.dtPedido) >= '2017-06-01'
    AND t2.idVendedor IS NOT NULL
),

tb_flSemVendaFuturo AS (
    SELECT '2017-06-01' AS dtRef, 
          t1.idvendedor,
          CASE WHEN t2.idvendedor IS NULL THEN 1 ELSE 0 END AS flSemVendaFuturo
    FROM tb_publico AS t1
    LEFT JOIN tb_publico_futuro AS t2
    ON t1.idvendedor = t2.idvendedor
),

abt_seller AS (
    SELECT *
    FROM tb_flsemvendafuturo
    LEFT JOIN workspace.olist.fs_seller_avaliacao USING (idvendedor, dtRef)
    LEFT JOIN workspace.olist.fs_seller_cliente USING (idvendedor, dtRef)
    LEFT JOIN workspace.olist.fs_seller_pagamento USING (idvendedor, dtRef)
    LEFT JOIN workspace.olist.fs_seller_produto USING (idvendedor, dtRef)
    LEFT JOIN workspace.olist.fs_seller_vendas USING (idvendedor, dtRef)
    LEFT JOIN workspace.olist.fs_seller_vendedor USING (idvendedor, dtRef)
)

SELECT * 
FROM abt_seller
