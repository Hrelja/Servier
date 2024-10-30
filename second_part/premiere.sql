SELECT date
    , SUM(prod_qty) AS ventes

FROM `project_id.dataset_id.TRANSACTION`

WHERE date between DATE(2019, 1, 1) AND DATE(2019, 1, 31)

GROUP BY 1