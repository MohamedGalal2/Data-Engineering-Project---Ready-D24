select brand,count(*) as brand_num
FROM ready-data-de24.dbt_mgalal_star.fact_table as t
group by brand
order by brand_num desc
limit 5