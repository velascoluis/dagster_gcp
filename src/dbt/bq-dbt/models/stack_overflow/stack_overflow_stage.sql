
{{ config(
    materialized='view'
)}}

SELECT *
FROM `bigquery-public-data.stackoverflow.posts_questions`


