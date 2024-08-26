--criando a view

CREATE VIEW IF NOT EXISTS sku_details_view AS
SELECT
    JSONExtractString(dado_linha, 'cod_prod') AS cod_prod,
    JSONExtractString(dado_linha, 'categoria') AS categoria,
    JSONExtractString(dado_linha, 'sub_categoria') AS sub_categoria,
    JSONExtractFloat(dado_linha, 'conteudo_valor') AS conteudo_valor,
    JSONExtractString(dado_linha, 'conteudo_medida') AS conteudo_medida
FROM
    working_data;
