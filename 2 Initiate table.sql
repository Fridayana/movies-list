CREATE TABLE public.movie_stars (
    movie_pubid character varying(128) NOT NULL,
    star_pubid character varying(128) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.movies (
    pubid character varying(128) DEFAULT gen_random_uuid() NOT NULL,
    title character varying(256) NOT NULL,
    release_year character varying(32) NOT NULL,
    one_line text,
    rating numeric,
    votes bigint DEFAULT 0,
    runtime bigint,
    gross numeric,
    genre text[],
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.stars (
    pubid character varying(128) DEFAULT gen_random_uuid() NOT NULL,
    name character varying(256) NOT NULL,
    role character varying(32),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
