-- nombre de shooting par an + par pays
WITH cte1 AS (

SELECT name AS country_shooting, EXTRACT(year FROM date_shoot), AS year_shooting, COUNT(*) AS number_of_shootings
FROM `meero-gcp-poc.test_dataset.shooting` AS t4
INNER JOIN `meero-gcp-poc.test_dataset.address` AS t5
ON t4.address_id = t5.id
INNER JOIN `meero-gcp-poc.test_dataset.country` AS t6
ON t5.country_code = t6.country_code
WHERE date_shoot is not null
GROUP BY country_shooting, year_shooting
ORDER BY country_shooting ASC),

-- nombre de photographes par an + par pays
cte2 AS (

SELECT name AS country_photographer, extract(year from date_creation) AS year_photographer, count(*) AS number_of_new_photographers
FROM `meero-gcp-poc.test_dataset.photographer` AS t1
INNER JOIN `meero-gcp-poc.test_dataset.address` AS t2
   ON t1.reference_address_id = t2.id
INNER JOIN `meero-gcp-poc.test_dataset.country`AS t3
   ON t2.country_code = t3.country_code
GROUP by country_photographer, year_photographer
ORDER BY country_photographer ASC)

-- final query

SELECT country_shooting, year_shooting, number_of_shootings, nb_new_photographers,
CASE WHEN running_total_photographers IS NULL THEN 0 ELSE running_total_photographers END AS running_total,
CASE WHEN ROUND(running_total_photographers / number_of_shootings, 2) IS NULL THEN 0 ELSE ROUND(running_total_photographers / number_of_shootings, 2) END AS dispo_ratio
FROM (
   SELECT
   country_shooting,
   year_shooting,
   number_of_shootings,
   CASE WHEN number_of_new_photographers IS NULL THEN 0 ELSE number_of_new_photographers END AS nb_new_photographers,
   SUM(number_of_new_photographers) OVER (PARTITION BY country_shooting ORDER BY year_shooting ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_photographers
   FROM cte1
   LEFT JOIN cte2
   ON cte1.country_shooting = cte2.country_photographer AND cte1.year_shooting = cte2.year_photographer
   ORDER BY country_shooting, year_shooting ASC)

