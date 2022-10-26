import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
from typing import Union, List, Tuple

c = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|

    Tabela wynikowa ma być posortowana po tylule filmu i języku.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
	    '''
    if isinstance(category_id, int):
        df =  pd.read_sql_query("SELECT title, language.name, category.name FROM film \
                                INNER JOIN film_category ON film.film_id = film_category.film_id \
                                INNER JOIN  category ON film_category.category_id = category.category_id \
                                INNER JOIN  language ON film.language_id = language.language_id \
                                WHERE film_category.category_id = " + str(category_id) + " \
                                ORDER BY film.title, language.name", c)
    else:
        df = None
    return df

def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category_id, int):
        df =  pd.read_sql_query("SELECT name, COUNT(film_category.film_id) FROM category \
                                INNER JOIN film_category ON category.category_id = film_category.category_id \
                                WHERE film_category.category_id = " + str(category_id) + " \
                                GROUP BY category.name", c)
    else:
        df = None
    return df

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not (isinstance(min_length, (int, float)) == False \
            or isinstance(max_length, (int, float)) == False \
            or max_length < min_length):
        df =  pd.read_sql_query("SELECT length, COUNT(title)FROM film \
                                WHERE length <= " + str(max_length) + " \
                                AND length >= " + str(min_length) + " \
                                GROUP BY film.length", c)
    else:
        df = None
    return df

def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams

    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(city, str):
        df =  pd.read_sql_query("SELECT city, customer.first_name, customer.last_name FROM city \
                                INNER JOIN address ON city.city_id = address.city_id \
                                INNER JOIN customer ON address.address_id = customer.address_id \
                                WHERE city.city = '{}' ORDER BY customer.first_name, customer.last_name".format(city), c)
    else:
        df = None
    return df

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389


    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(length, (int, float)):
        df =  pd.read_sql_query("SELECT length, AVG(payment.amount) FROM film \
                                INNER JOIN inventory ON film.film_id = inventory.film_id  \
                                INNER JOIN rental ON inventory.inventory_id = rental.inventory_id \
                                INNER JOIN payment ON rental.rental_id = payment.rental_id \
                                WHERE length = " + str(length) + " GROUP BY length", c)
    else:
        df = None
    return df

def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265

    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(sum_min, (int, float)):
        df =  pd.read_sql_query("SELECT first_name, last_name, SUM(film.length) FROM customer \
                                INNER JOIN rental ON customer.customer_id = rental.customer_id \
                                INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id \
                                INNER JOIN film ON inventory.film_id = film.film_id \
                                GROUP BY first_name, last_name \
                                HAVING SUM(film.length) > " + str(sum_min) + " \
                                ORDER BY first_name, last_name, sum", c)
    else:
        df = None
    return df

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(name, str):
        df =  pd.read_sql_query("SELECT name, AVG(film.length), SUM(film.length), MAX(film.length), MIN(film.length) FROM category \
                                INNER JOIN film_category ON category.category_id = film_category.category_id \
                                INNER JOIN film ON film_category.film_id = film.film_id \
                                WHERE category.name = '{}' GROUP BY category.name".format(name), c)
    else:
        df = None
    return df
