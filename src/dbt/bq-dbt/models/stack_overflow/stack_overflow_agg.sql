
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table'
)}}



SELECT a.id, title, c files, answer_count answers, favorite_count favs,
view_count views, score
FROM {{ref('stack_overflow_stage')}} a
JOIN (
  SELECT CAST(REGEXP_EXTRACT(content,r'stackoverflow.com/questions/([0-9]+)/') AS INT64) id, COUNT(*) c,
MIN(sample_path) sample_path
  FROM `fh-bigquery.github_extracts.contents_js`
  WHERE content LIKE '%stackoverflow.com/questions/%'
  GROUP BY 1
  HAVING id>0
  ORDER BY 2 DESC
) b
ON a.id=b.id
ORDER BY c DESC

