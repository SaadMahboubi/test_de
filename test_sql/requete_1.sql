SELECT
    date AS date_vente,
    SUM(prod_price * prod_qty) AS ventes
FROM TRANSACTIONS
WHERE date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY date
ORDER BY date;
