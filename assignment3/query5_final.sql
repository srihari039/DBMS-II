DROP TABLE IF EXISTS temp_table;
DROP TABLE IF EXISTS temp2_table;

CREATE TABLE  temp_table AS (
SELECT public.authored_by.paper_id,author_id
FROM public.authored_by
INNER JOIN(
		SELECT COUNT(*)c,paper_id
		FROM public.authored_by
		GROUP BY paper_id
		)temp
ON authored_by.paper_id = temp.paper_id
WHERE c>1);


CREATE TABLE temp2_table AS(
SELECT t1.paper_id,t1.author_id as author1, t2.author_id as author2, t3.name As author1_name, t4.name As author2_name
FROM temp_table t1
JOIN temp_table t2
ON t1.paper_id = t2.paper_id and t1.author_id < t2.author_id
LEFT JOIN public.author t3
ON t1.author_id=t3.author_id
LEFT JOIN public.author t4
ON t2.author_id=t4.author_id
ORDER BY paper_id ASC,author1 ASC,author2 ASC);

SELECT author1,author2,author1_name,author2_name ,COUNT(*)
FROM temp2_table
GROUP BY author1,author2,author1_name,author2_name
HAVING COUNT(*)>1
ORDER BY COUNT(*) ASC,author1 ASC,author2 ASC;







