DROP TABLE IF EXISTS temp_table;
DROP TABLE IF EXISTS ref_table;

CREATE TABLE  temp_table AS (
SELECT ref_id,public.reference_papers.paper_id,public.author.author_id,public.authored_by.priority,name AS author_name,public.research_paper.abstract,public.research_paper.year,title,conference_name 
FROM public.reference_papers LEFT JOIN public.authored_by 
ON public.reference_papers.paper_id=public.authored_by.paper_id 
LEFT JOIN public.research_paper
ON public.reference_papers.paper_id = public.research_paper.paper_id
LEFT JOIN public.author 
ON public.authored_by.author_id=public.author.author_id
LEFT JOIN public.held_at
ON public.held_at.paper_id=public.reference_papers.paper_id 
LEFT JOIN public.conference
ON public.held_at.conference_id=public.conference.conference_id
ORDER BY ref_id ASC, public.reference_papers.paper_id ASC, public.authored_by.priority ASC);  

CREATE TABLE ref_table AS (
SELECT ref_id,paper_id,string_agg(author_name,',') AS author_names, abstract,year,title, conference_name
FROM temp_table
GROUP BY ref_id,paper_id,abstract,year,title,conference_name);

SELECT t1.ref_id as paper1_id,t2.paper_id as cited_by_paper2_id, t2.author_names AS paper2_author_names, t2.abstract AS paper2_abstract,t2.year AS paper2_year,t2.title AS paper2_title, t2.conference_name as paper2_conference
FROM ref_table t1
INNER JOIN ref_table t2 ON t1.paper_id=t2.ref_id
ORDER BY t1.ref_id ASC, t2.paper_id ASC;
