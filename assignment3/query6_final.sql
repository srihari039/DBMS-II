CREATE OR REPLACE VIEW temp1 AS
SELECT Au1.author_id AS A1, Au1.paper_id AS P1, public.reference_papers.ref_id AS P2
FROM public.authored_by Au1
INNER JOIN public.reference_papers
ON Au1.paper_id = public.reference_papers.paper_id;

CREATE OR REPLACE VIEW temp2 AS
SELECT temp1.A1, temp1.P1, temp1.P2, Au2.author_id AS A2, public.reference_papers.ref_id AS P3
FROM temp1
INNER JOIN public.authored_by Au2
ON temp1.P2 = Au2.paper_id
INNER JOIN public.reference_papers
ON public.reference_papers.paper_id = temp1.P2;

CREATE OR REPLACE VIEW temp3 AS
SELECT temp2.A1, temp2.P1, temp2.A2, temp2.P2, temp2.P3, Au3.author_id AS A3, public.reference_papers.ref_id AS P4
FROM temp2
INNER JOIN public.authored_by Au3
ON temp2.P3 = Au3.paper_id
INNER JOIN public.reference_papers
ON public.reference_papers.paper_id = temp2.P3;

CREATE OR REPLACE VIEW temp4 AS
SELECT temp3.A1, temp3.P1, temp3.A2, temp3.P2, temp3.A3, temp3.P3, temp3.P4, Au4.author_id AS A4
FROM temp3
INNER JOIN public.authored_by Au4
ON temp3.P4 = Au4.paper_id AND Au4.author_id = A1;

CREATE OR REPLACE VIEW author_triples AS
SELECT A1 as author1, A2 as author2, A3 as author3,N1.name as name1, N2.name as name2, N3.name as name3
FROM temp4
LEFT JOIN public.author N1
ON temp4.A1=N1.author_id
LEFT JOIN public.author N2
ON temp4.A2=N2.author_id
LEFT JOIN public.author N3
ON temp4.A3=N3.author_id
WHERE A1<A2 AND A2<A3;

SELECT author1,author2,author3,name1,name2,name3,COUNT(*)
FROM author_triples
GROUP BY author1,author2,author3,name1,name2,name3
ORDER BY COUNT(*) DESC,author1 ASC,author2 ASC,author3 ASC;