---------------------------------------------4.a---------------------------------------------
CREATE VIEW public.number_of_unique_film_title AS
 SELECT count(DISTINCT title) AS "Total"
   FROM public.movies;


---------------------------------------------4.b---------------------------------------------
CREATE VIEW public.list_of_films_starring_lena_headey AS
 SELECT m.title AS "Film Title",
    m.release_year AS "Year of Release",
    m.rating AS "Rating"
   FROM ((public.movies m
     LEFT JOIN public.movie_stars ms ON (((m.pubid)::text = (ms.movie_pubid)::text)))
     LEFT JOIN public.stars s ON (((ms.star_pubid)::text = (s.pubid)::text)))
  WHERE (((s.role)::text = 'Star'::text) AND ((s.name)::text ~~* '%Lena Headey%'::text))
  ORDER BY m.release_year DESC;


---------------------------------------------4.c---------------------------------------------
CREATE VIEW public.list_of_directors_and_total_gross_of_their_films AS
 SELECT s.name AS "Director",
    sum(COALESCE(m.gross, (0)::numeric)) AS "Total Gross"
   FROM ((public.stars s
     JOIN public.movie_stars ms ON (((s.pubid)::text = (ms.star_pubid)::text)))
     LEFT JOIN public.movies m ON (((m.pubid)::text = (ms.movie_pubid)::text)))
  WHERE ((s.role)::text = 'Director'::text)
  GROUP BY s.name
  ORDER BY (sum(COALESCE(m.gross, (0)::numeric))) DESC;


---------------------------------------------4.d---------------------------------------------

CREATE VIEW public.top_five_comedy_films_by_gross AS
 SELECT title,
    release_year,
    rating,
    gross
   FROM public.movies m
  WHERE ('Comedy'::text = ANY (genre))
  ORDER BY
        CASE
            WHEN (gross IS NOT NULL) THEN 1
            ELSE 2
        END, gross DESC
 LIMIT 5;


---------------------------------------------4.e---------------------------------------------

CREATE VIEW public.films_directed_by_martin_scorsese_and_starring_robert_de_niro AS
 SELECT m.title,
    m.release_year,
    m.rating
   FROM ((public.movies m
     JOIN public.movie_stars ms ON (((m.pubid)::text = (ms.movie_pubid)::text)))
     LEFT JOIN public.stars s ON (((ms.star_pubid)::text = (s.pubid)::text)))
  WHERE ((((s.role)::text = 'Director'::text) AND ((s.name)::text = 'Martin Scorsese'::text)) OR (((s.role)::text = 'Star'::text) AND ((s.name)::text = 'Robert De Niro'::text)))
  GROUP BY m.title, m.release_year, m.rating
 HAVING (count(*) = 2);
