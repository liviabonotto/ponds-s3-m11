CREATE VIEW IF NOT EXISTS product_summary AS
SELECT 
    categoria, 
    sub_categoria, 
    COUNT(*) as product_count, 
    AVG(conteudo_valor) as avg_value
FROM working_data
GROUP BY categoria, sub_categoria;
