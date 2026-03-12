-- Databricks notebook source
WITH tb_base AS (
    SELECT v.idVendedor,
          p.dtPedido,
          pr.descCategoria,
          pr.idProduto,
          pr.nrTamanhoNome,
          pr.nrTamanhoDescricao,
          pr.nrFotos,
          pr.vlAlturaCm * pr.vlComprimentoCm * pr.vlLarguraCm AS vlCm3Prod,
          pr.vlPesoGramas,
          ip.vlPreco + ip.vlFrete AS vlPagamento
          
    FROM workspace.olist.pedido AS p
    LEFT JOIN workspace.olist.item_pedido AS ip
    ON p.idPedido = ip.idPedido

    LEFT JOIN workspace.olist.vendedor AS v
    on ip.idVendedor = v.idVendedor

    LEFT JOIN workspace.olist.produto AS pr
    ON ip.idProduto = pr.idProduto

    WHERE date(p.dtPedido) < '2017-06-01'
    AND pr.descCategoria IS NOT NULL
),
tb_summarySeller AS (
    SELECT idVendedor,
          COUNT(DISTINCT idProduto) AS qtdeCatalogo,
          COUNT (DISTINCT descCategoria) AS qtdeCategoria,
          SUM(vlPagamento) AS gmvSeller,
          AVG(nrTamanhoNome) AS mediaTamanhoNome,
          AVG(nrTamanhoDescricao) AS mediaTamanhoDescricao,
          AVG(vlCm3Prod) AS mediaCm3Prod,
          AVG(vlPesoGramas) AS mediaPesoGramas,
          AVG(CASE WHEN nrFotos IS NOT NULL THEN nrFotos ELSE 0 END) AS mediaFotosProd,
          AVG(CASE WHEN nrTamanhoDescricao IS NOT NULL THEN 1 ELSE 0 END) AS mediaTemDescricaoProd
    FROM tb_base

    GROUP BY idVendedor
),
tb_summarySellerCat AS (
    SELECT idVendedor,
          descCategoria,
          COUNT(DISTINCT idProduto) AS qtdeProdutos,
          SUM(vlPagamento) AS gmv
    FROM tb_base
    GROUP BY idVendedor, descCategoria
),
tb_summarySellerTopcat AS (
    SELECT DISTINCT idVendedor,
          FIRST_VALUE(descCategoria) OVER (PARTITION BY idVendedor ORDER BY qtdeProdutos) AS topCatVendas,
          FIRST_VALUE(descCategoria) OVER (PARTITION BY idVendedor ORDER BY gmv) AS topCatGMV
    FROM tb_summarySellerCat
),
tb_summaryCat AS (
  SELECT descCategoria,
         SUM(gmv) AS gmvCategoria
  FROM tb_summarySellerCat
  GROUP BY descCategoria
),
tb_summarySellerShare AS (
  SELECT sc.idVendedor,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'cine_foto' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'cine_foto' THEN c.gmvCategoria END), 0) AS shareGMVcine_foto,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'pet_shop' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'pet_shop' THEN c.gmvCategoria END), 0) AS shareGMVpet_shop,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'construcao_ferramentas_jardim' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'construcao_ferramentas_jardim' THEN c.gmvCategoria END), 0) AS shareGMVconstrucao_ferramentas_jardim,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fashion_roupa_masculina' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fashion_roupa_masculina' THEN c.gmvCategoria END), 0) AS shareGMVfashion_roupa_masculina,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fashion_esporte' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fashion_esporte' THEN c.gmvCategoria END), 0) AS shareGMVfashion_esporte,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'informatica_acessorios' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'informatica_acessorios' THEN c.gmvCategoria END), 0) AS shareGMVinformatica_acessorios,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'consoles_games' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'consoles_games' THEN c.gmvCategoria END), 0) AS shareGMVconsoles_games,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'eletroportateis' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'eletroportateis' THEN c.gmvCategoria END), 0) AS shareGMVeletroportateis,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'moveis_sala' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'moveis_sala' THEN c.gmvCategoria END), 0) AS shareGMVmoveis_sala,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'artigos_de_natal' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'artigos_de_natal' THEN c.gmvCategoria END), 0) AS shareGMVartigos_de_natal,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'sinalizacao_e_seguranca' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'sinalizacao_e_seguranca' THEN c.gmvCategoria END), 0) AS shareGMVsinalizacao_e_seguranca,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'cool_stuff' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'cool_stuff' THEN c.gmvCategoria END), 0) AS shareGMVcool_stuff,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fashion_calcados' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fashion_calcados' THEN c.gmvCategoria END), 0) AS shareGMVfashion_calcados,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fashion_underwear_e_moda_praia' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fashion_underwear_e_moda_praia' THEN c.gmvCategoria END), 0) AS shareGMVfashion_underwear_e_moda_praia,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'telefonia_fixa' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'telefonia_fixa' THEN c.gmvCategoria END), 0) AS shareGMVtelefonia_fixa,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'artes' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'artes' THEN c.gmvCategoria END), 0) AS shareGMVartes,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fashion_roupa_infanto_juvenil' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fashion_roupa_infanto_juvenil' THEN c.gmvCategoria END), 0) AS shareGMVfashion_roupa_infanto_juvenil,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'construcao_ferramentas_seguranca' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'construcao_ferramentas_seguranca' THEN c.gmvCategoria END), 0) AS shareGMVconstrucao_ferramentas_seguranca,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'seguros_e_servicos' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'seguros_e_servicos' THEN c.gmvCategoria END), 0) AS shareGMVseguros_e_servicos,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fashion_bolsas_e_acessorios' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fashion_bolsas_e_acessorios' THEN c.gmvCategoria END), 0) AS shareGMVfashion_bolsas_e_acessorios,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'casa_conforto' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'casa_conforto' THEN c.gmvCategoria END), 0) AS shareGMVcasa_conforto,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'tablets_impressao_imagem' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'tablets_impressao_imagem' THEN c.gmvCategoria END), 0) AS shareGMVtablets_impressao_imagem,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'moveis_cozinha_area_de_servico_jantar_e_jardim' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'moveis_cozinha_area_de_servico_jantar_e_jardim' THEN c.gmvCategoria END), 0) AS shareGMVmoveis_cozinha_area_de_servico_jantar_e_jardim,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fashion_roupa_feminina' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fashion_roupa_feminina' THEN c.gmvCategoria END), 0) AS shareGMVfashion_roupa_feminina,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'moveis_colchao_e_estofado' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'moveis_colchao_e_estofado' THEN c.gmvCategoria END), 0) AS shareGMVmoveis_colchao_e_estofado,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'beleza_saude' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'beleza_saude' THEN c.gmvCategoria END), 0) AS shareGMVbeleza_saude,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'climatizacao' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'climatizacao' THEN c.gmvCategoria END), 0) AS shareGMVclimatizacao,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'pcs' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'pcs' THEN c.gmvCategoria END), 0) AS shareGMVpcs,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'brinquedos' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'brinquedos' THEN c.gmvCategoria END), 0) AS shareGMVbrinquedos,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'relogios_presentes' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'relogios_presentes' THEN c.gmvCategoria END), 0) AS shareGMVrelogios_presentes,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'livros_tecnicos' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'livros_tecnicos' THEN c.gmvCategoria END), 0) AS shareGMVlivros_tecnicos,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'pc_gamer' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'pc_gamer' THEN c.gmvCategoria END), 0) AS shareGMVpc_gamer,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'casa_construcao' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'casa_construcao' THEN c.gmvCategoria END), 0) AS shareGMVcasa_construcao,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'agro_industria_e_comercio' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'agro_industria_e_comercio' THEN c.gmvCategoria END), 0) AS shareGMVagro_industria_e_comercio,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'automotivo' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'automotivo' THEN c.gmvCategoria END), 0) AS shareGMVautomotivo,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'ferramentas_jardim' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'ferramentas_jardim' THEN c.gmvCategoria END), 0) AS shareGMVferramentas_jardim,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'moveis_escritorio' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'moveis_escritorio' THEN c.gmvCategoria END), 0) AS shareGMVmoveis_escritorio,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'alimentos' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'alimentos' THEN c.gmvCategoria END), 0) AS shareGMValimentos,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'livros_importados' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'livros_importados' THEN c.gmvCategoria END), 0) AS shareGMVlivros_importados,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'dvds_blu_ray' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'dvds_blu_ray' THEN c.gmvCategoria END), 0) AS shareGMVdvds_blu_ray,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'fraldas_higiene' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'fraldas_higiene' THEN c.gmvCategoria END), 0) AS shareGMVfraldas_higiene,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'industria_comercio_e_negocios' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'industria_comercio_e_negocios' THEN c.gmvCategoria END), 0) AS shareGMVindustria_comercio_e_negocios,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'telefonia' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'telefonia' THEN c.gmvCategoria END), 0) AS shareGMVtelefonia,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'market_place' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'market_place' THEN c.gmvCategoria END), 0) AS shareGMVmarket_place,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'moveis_quarto' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'moveis_quarto' THEN c.gmvCategoria END), 0) AS shareGMVmoveis_quarto,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'perfumaria' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'perfumaria' THEN c.gmvCategoria END), 0) AS shareGMVperfumaria,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'esporte_lazer' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'esporte_lazer' THEN c.gmvCategoria END), 0) AS shareGMVesporte_lazer,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'construcao_ferramentas_construcao' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'construcao_ferramentas_construcao' THEN c.gmvCategoria END), 0) AS shareGMVconstrucao_ferramentas_construcao,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'eletrodomesticos' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'eletrodomesticos' THEN c.gmvCategoria END), 0) AS shareGMVeletrodomesticos,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'instrumentos_musicais' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'instrumentos_musicais' THEN c.gmvCategoria END), 0) AS shareGMVinstrumentos_musicais,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'artes_e_artesanato' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'artes_e_artesanato' THEN c.gmvCategoria END), 0) AS shareGMVartes_e_artesanato,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'alimentos_bebidas' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'alimentos_bebidas' THEN c.gmvCategoria END), 0) AS shareGMValimentos_bebidas,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'moveis_decoracao' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'moveis_decoracao' THEN c.gmvCategoria END), 0) AS shareGMVmoveis_decoracao,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'malas_acessorios' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'malas_acessorios' THEN c.gmvCategoria END), 0) AS shareGMVmalas_acessorios,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'musica' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'musica' THEN c.gmvCategoria END), 0) AS shareGMVmusica,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'casa_conforto_2' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'casa_conforto_2' THEN c.gmvCategoria END), 0) AS shareGMVcasa_conforto_2,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'construcao_ferramentas_ferramentas' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'construcao_ferramentas_ferramentas' THEN c.gmvCategoria END), 0) AS shareGMVconstrucao_ferramentas_ferramentas,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'cama_mesa_banho' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'cama_mesa_banho' THEN c.gmvCategoria END), 0) AS shareGMVcama_mesa_banho,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'eletronicos' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'eletronicos' THEN c.gmvCategoria END), 0) AS shareGMVeletronicos,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'eletrodomesticos_2' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'eletrodomesticos_2' THEN c.gmvCategoria END), 0) AS shareGMVeletrodomesticos_2,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'livros_interesse_geral' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'livros_interesse_geral' THEN c.gmvCategoria END), 0) AS shareGMVlivros_interesse_geral,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'bebidas' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'bebidas' THEN c.gmvCategoria END), 0) AS shareGMVbebidas,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'bebes' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'bebes' THEN c.gmvCategoria END), 0) AS shareGMVbebes,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'utilidades_domesticas' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'utilidades_domesticas' THEN c.gmvCategoria END), 0) AS shareGMVutilidades_domesticas,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'papelaria' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'papelaria' THEN c.gmvCategoria END), 0) AS shareGMVpapelaria,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'audio' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'audio' THEN c.gmvCategoria END), 0) AS shareGMVaudio,
  COALESCE(SUM(CASE WHEN sc.descCategoria = 'la_cuisine' THEN sc.gmv ELSE 0 END) / SUM(CASE WHEN c.descCategoria = 'la_cuisine' THEN c.gmvCategoria END), 0) AS shareGMVla_cuisine
  FROM tb_summarySellerCat AS sc
  LEFT JOIN tb_summaryCat AS c
  ON sc.descCategoria = c.descCategoria
  GROUP BY sc.idVendedor
),
tb_join AS (
  SELECT * 
  FROM tb_summaryseller AS s
  LEFT JOIN tb_summarysellertopcat AS tc USING (idVendedor)
  LEFT JOIN tb_summarysellershare AS ss USING (idVendedor)
  ORDER BY s.idVendedor
)
SELECT *
FROM tb_join


-- COMMAND ----------


