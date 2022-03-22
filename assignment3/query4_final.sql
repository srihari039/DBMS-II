SELECT ref_id AS paper_id,COUNT(*)
FROM public.reference_papers
WHERE paper_id IS NOT NULL and ref_id IS NOT NULL
GROUP BY ref_id
ORDER BY COUNT DESC
LIMIT 20;
