SELECT
    t.client_id as client_id,
    SUM(CASE WHEN p.product_type = 'MEUBLE' THEN t.prod_price * t.prod_qty ELSE 0 END) AS ventes_meuble,
    SUM(CASE WHEN p.product_type = 'DECO' THEN t.prod_price * t.prod_qty ELSE 0 END) AS ventes_deco
FROM TRANSACTIONS t
INNER JOIN PRODUCT_NOMENCLATURE p ON t.prod_id = p.product_id
WHERE t.date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY t.client_id
ORDER BY t.client_id;
