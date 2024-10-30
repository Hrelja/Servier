SELECT client_id
    , SUM(CASE WHEN product_type = 'MEUBLE' THEN prod_qty ELSE 0 END) AS ventes_meuble
    , SUM(CASE WHEN product_type = 'DECO' THEN prod_qty ELSE 0 END) AS ventes_deco

FROM `project_id.dataset_id.TRANSACTION` transactions

LEFT JOIN `project_id.dataset_id.PRODUCT_NOMENCLATURE` product_nomenclature 
ON transactions.prod_id = product_nomenclature.product_id

WHERE transactions.date between DATE(2019, 1, 1) AND DATE(2019, 1, 31)

GROUP BY 1