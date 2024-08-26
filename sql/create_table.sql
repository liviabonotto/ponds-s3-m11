CREATE TABLE IF NOT EXISTS working_data (
    cod_prod String,
    nome_abrev String,
    nome_completo String,
    descricao String,
    categoria String,
    sub_categoria String,
    marca String,
    conteudo_valor Float32,
    conteudo_medida String
) ENGINE = MergeTree()
ORDER BY cod_prod;
