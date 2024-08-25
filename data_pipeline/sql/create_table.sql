--nossos dados raw ficam no bucket (o que recebemos) e o working_data é quando começamos a trabalhar nos processos--

CREATE TABLE IF NOT EXISTS working_data (
    data_ingestao DateTime,
    dado_linha String,
    tag String
) ENGINE = MergeTree()
ORDER BY data_ingestao;