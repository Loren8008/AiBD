import numpy as np
import pickle
import pandas as pd
import psycopg2 as pg
c = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

from typing import Union, List, Tuple

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli category jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|

    Tabela wynikowa ma być posortowana po tylule filmu i języku.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category) == int:
        df =  pd.read_sql_query("SELECT film.title AS title, language.name AS languge, category.name AS category FROM film \
				INNER JOIN language ON film.language_id = language.language_id \
				INNER JOIN film_category ON film.film_id = film_category.film_id \
				INNER JOIN category ON film_category.category_id = category.category_id \
				WHERE film_category.category_id = '" + str(category) + "' \
				ORDER BY film.title, language.name ASC", c)
    elif type(category) == str:
        df =  pd.read_sql_query("SELECT film.title AS title, language.name AS languge, category.name AS category FROM film \
				INNER JOIN language ON film.language_id = language.language_id \
				INNER JOIN film_category ON film.film_id = film_category.film_id \
				INNER JOIN category ON film_category.category_id = category.category_id \
				WHERE category.name = '" + category + "' \
				ORDER BY film.title, language.name ASC", c)
    else:
        df = None
    return df
    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    if isinstance(category, int):
        insert = ("WHERE category.category_id = " + str(category))
    elif isinstance(category, str):
        insert = ("WHERE category.name ~* '" + category + "'")
    else:
        insert = None
    if insert == None:
        df = None
    else:
        df = pd.read_sql_query("SELECT film.title, language.name AS languge, category.name AS category FROM film \
    	  INNER JOIN language ON film.language_id = language.language_id \
    	  INNER JOIN film_category ON film.film_id = film_category.film_id \
    	  INNER JOIN category ON film_category.category_id = category.category_id " + insert + " \
          ORDER BY film.title, language.name ASC", c)
    return df           
    
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    if isinstance(title, str):              
        df = pd.read_sql_query("SELECT first_name, last_name FROM actor \
    	  INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id \
    	  INNER JOIN film ON film_actor.film_id = film.film_id \
    	  WHERE film.title LIKE '" + title + "' \
          ORDER BY last_name, first_name ASC", c)
    else:
        df = None
    return df        

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    if isinstance(words, list):    
        words_str = ""
        for word in words:
            words_str += word
            words_str += "|"
        words_str = words_str[:-1]
        df = pd.read_sql_query("select title from film \
                               where title ~* '(?:^| )(" + words_str + ")(?:$| )' ", c)
        #df = pd.read_sql_query(f"SELECT title FROM film \
        #                       WHERE title SIMILAR TO '%" + words_str + "%' \
        #                       OR title SIMILAR TO '" + words_str + "' \
        #                       OR title SIMILAR TO '%" + words_str + "' \
        #                       OR title SIMILAR TO '" + words_str + "%'", c)
       #WHERE title ~* '({" + words_str + "}){1,}'", c)
    else:
        df = None
    return df        


