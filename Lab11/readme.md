1. Ile rekordów znajduje się w tabeli nyc_streets?
19091
2. Ile ulic w Nowym Jorku ma nazwy zaczynające się na „B”, „Q” i „M”?
2102
3. Jaka jest populacja miasta Nowy Jork?
expression: sum("popn_total")
8175032
4. Jaka jest populacja Bronxu, Manhattanu i Queens?
expression: sum("popn_total",filter:="boroname"='Queens' or "boroname"='Manhattan' or "boroname"='The Bronx')
Suma: 5201602
5. Ile dzielnic (neighborhoods) znajduje się w każdej gminie (borough)?
expression: count('name',group_by:='boroname',filter:="boroname"='The Bronx')
Brooklyn: 23
Manhattan: 28
Queens: 30
Staten Island: 24
The Bronx: 24
