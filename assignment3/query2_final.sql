
SELECT public.reference_papers.paper_id,ref_id,public.author.author_id,public.authored_by.priority,name AS author_name,public.research_paper.abstract,public.research_paper.year,title,conference_name 
FROM public.reference_papers JOIN public.authored_by 
ON public.reference_papers.ref_id=public.authored_by.paper_id 
JOIN public.research_paper
ON public.reference_papers.ref_id = public.research_paper.paper_id
JOIN public.author 
ON public.authored_by.author_id=public.author.author_id
JOIN public.held_at
ON public.held_at.paper_id=public.reference_papers.ref_id 
JOIN public.conference
ON public.held_at.conference_id=public.conference.conference_id
ORDER BY public.reference_papers.paper_id ASC,ref_id ASC,public.authored_by.priority ASC;  
