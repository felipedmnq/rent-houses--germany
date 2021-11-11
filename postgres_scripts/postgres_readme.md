## Postgres Database.

This solution was build to extract the needed information from the website (https://www.immonet.de/) and store it localy in a postgres database.


The first step of the project was build the extration part of the data pipeline. Three python scripts were written to perform the task. 
It could be done in a simplier way, but the real intention here was to practice and try some new packeges using python to connect with a postgres database, 
load data into the database eng get data from the database. 

The first script (```get_offers_by_city.py```) gets the quantity of rent offers for 11 german cities and load the data 
into the database in a cumulative way (append) to build a "historical table".



The table below ilustrates the extracted data:

<p align="center">
  <img width="449" alt="Screen Shot 2021-11-10 at 20 59 18" src="https://user-images.githubusercontent.com/71295866/141184872-59eece7e-fd4d-456c-8366-66dbbe9928e1.png">
</p>


The second script (```get_offer_ids.py```) get the previous table from the database and use it as base to run each offer for each city and extract the "offer id".
All ids from each offer are stored in a new table and load into the database.

The following table shows a sample of the returned data.

<p align="center">
  <img width="530" alt="Screen Shot 2021-11-10 at 21 10 55" src="https://user-images.githubusercontent.com/71295866/141186226-b9948549-9489-4aa4-9984-80ed31b27f8b.png">
</p> 

The third script (```get_offers_infos.py```) uses the previous one (use the offer ids) to extract all main information from each offer from each city. 
All the information is extracted in a raw format, all the main information comes in a array to be tranformed and cleaned later. 

<p align="center">
  <img width="1431" alt="Screen Shot 2021-11-10 at 21 22 05" src="https://user-images.githubusercontent.com/71295866/141187696-67002d2e-f1fb-409e-97de-e66726b4fddd.png">
</p>

As I said previously, it could be done in a much easier way, but the main goal here was to practice the connection with the database, 
load data into the database and extract data from the database using python.

From here we have the base data for further work.
