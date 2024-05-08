import pandas as pd
import re
import uuid
from core import db_connect, db_insert


def transform_data(list_data):
    movie_list = []
    stars_list = []
    movie_star_list = []
    for data in list_data:
        movie_data = {}
        movie_data['pubid'] = str(uuid.uuid4())
        movie_data['title'] = data['MOVIES'].strip()
        movie_data['year'] = re.sub("[^0-9]", '', data['YEAR']) if str(data['YEAR']) != 'nan' else '-'
        if len(str(movie_data['year'])) > 4:
            movie_data['year'] = '-'
        movie_data['rating'] = data['RATING'] if str(data['RATING']) != 'nan' else None
        movie_data['one_line'] = data['ONE-LINE'] if str(data['ONE-LINE']) != 'nan' else None
        movie_data['votes'] = int(data['VOTES'].replace(',', '')) if str(data['VOTES']) != 'nan' else None
        movie_data['runtime'] = data['RunTime'] if str(data['RunTime']) != 'nan' else None
        movie_data['gross'] = float(data['Gross'][1:-1]) if str(data['Gross']) != 'nan' else None
        movie_data['genre'] = '{' + data['GENRE'].replace(' ', '') + '}' if str(data['GENRE']) != 'nan' else None
        movie_list.append(movie_data)

        if str(data['STARS']) != 'nan':
            data['STARS'] = data['STARS'].replace('\n', '')
            stars = data['STARS'].split('|')  # STAR - DIRECTOR
            for star in stars:
                if 'director' in star.lower():
                    list_name = star.split(':')
                    for name in list_name[1].split(','):
                        star_data = {}
                        movie_star_data = {}
                        movie_star_data['movie'] = movie_data['title']
                        movie_star_data['year'] = movie_data['year']
                        star_data['pubid'] = str(uuid.uuid4())
                        star_data['name'] = name.strip()
                        star_data['role'] = 'Director'
                        stars_list.append(star_data)
                        movie_star_data['name'] = star_data['name']
                        movie_star_data['role'] = star_data['role']
                        movie_star_list.append(movie_star_data)
                elif 'star' in star.lower():
                    list_name = star.split(':')
                    for name in list_name[1].split(','):
                        star_data = {}
                        movie_star_data = {}
                        movie_star_data['movie'] = movie_data['title']
                        movie_star_data['year'] = movie_data['year']
                        star_data['pubid'] = str(uuid.uuid4())
                        star_data['name'] = name.strip()
                        star_data['role'] = 'Star'
                        stars_list.append(star_data)
                        movie_star_data['name'] = star_data['name']
                        movie_star_data['role'] = star_data['role']
                        movie_star_list.append(movie_star_data)

    return movie_list, stars_list, movie_star_list


def insert_movie(db_cursor, movie_list):
    query = f"""
            INSERT INTO public.movies (pubid, title, release_year, rating, one_line, votes, runtime, gross, genre)
            VALUES (%(pubid)s, %(title)s, %(year)s, %(rating)s, %(one_line)s, %(votes)s, %(runtime)s, %(gross)s, %(genre)s)
            ON CONFLICT (title, release_year)
            DO NOTHING;
            """
    db_insert(db_curr=db_cursor, list_data=movie_list, sql=query)


def insert_stars(db_cursor, star_list):
    query = f"""
            INSERT INTO public.stars (pubid, name, role)
            VALUES (%(pubid)s, %(name)s, %(role)s)
            ON CONFLICT (name, role)
            DO NOTHING;
            """
    db_insert(db_curr=db_cursor, list_data=star_list, sql=query)


def insert_moviestars(db_cursor, moviestar_list):
    query = f"""
            with cte as (
                select %(movie)s title, %(year)s as year, pubid
                from public.stars s
                where name = %(name)s
                and role = %(role)s
            ),
            tbl as (
                select m.pubid as movie, cte.pubid
                from cte
                left join public.movies m on cte.title = m.title and cte.year = m.release_year
                where m.title = cte.title
                and release_year = cte.year
            )
            INSERT INTO public.movie_stars (movie_pubid, star_pubid)
            select tbl.movie, tbl.pubid
            from tbl
            ON CONFLICT DO NOTHING;
            """
    db_insert(db_curr=db_cursor, list_data=moviestar_list, sql=query)


if __name__ == '__main__':
    df = pd.read_csv(r'data/movies.csv')
    list_data = df.to_dict('records')
    if list_data:
        db_conn = db_connect(db_string='pg.local')
        db_curr = db_conn.cursor()
        movies, stars, moviestar = transform_data(list_data)
        insert_movie(db_curr, movies)
        insert_stars(db_curr, stars)
        insert_moviestars(db_curr, moviestar)
        db_curr.close()
        db_conn.close()
    else:
        print("There's No Data.")
