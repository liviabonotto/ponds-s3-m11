CREATE VIEW IF NOT EXISTS sku_price_view AS
SELECT
    data_ingestao AS ingestion_date,
    JSONExtractString(dado_linha, 'cod_prod') AS cod_prod,
    JSONExtractString(dado_linha, 'data_inicio') AS data_inicio_preco,
    JSONExtractString(dado_linha, 'data_fim') AS data_fim_preco,
    JSONExtractFloat(dado_linha, 'preco') AS preco,
    tag
FROM
    working_data
WHERE
    tag = 'sku_price';
