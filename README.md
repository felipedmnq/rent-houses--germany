## Postgres Database.

This solution was build to extract the needed information from the website (https://www.immonet.de/) and store it localy in a postgres database.

<p align="center">
  <img width="630" alt="Screen Shot 2021-11-11 at 17 46 57" src="https://user-images.githubusercontent.com/71295866/141341515-b883923e-9843-4572-84a2-00856fc22d9e.png">
</p>


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

## Data Analasysis

### Hypotesis

**H1. Offers with larger areas are more expensive.**

As expected, this hypothesis is **FALSE**.

As we can see in the chart below, as the area size increases, the rent average price increses as well and this is natural, places with bigger areas should have a bigger cost.

<p align="center">
  <img width="1431" alt="avg_rent_by_m2.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/charts/avg_rent_by_m2.png?raw=true">
</p>

But when we analyze the average price per m2 for each range of area, the data shows us that not necessarily the bigger places are more expensive, they have a bigger cost, but the most expensive places are the smallest ones, they have a much bigger cost by m2 than the medium to bigger places.

<p align="center">
  <img width="1431" alt="avg_m2_by_range_area.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/charts/avg_m2_by_range_area.png?raw=true">
</p>

**H2. The more rooms, the more expensive.**

The situation here is similar than the previous and the hypotesys is aldo **FALSE**. 

When we analyze the rent cost and the number of rooms, is natural that as bigger the number of room, the bigger the rent cost and the next chart shows us this. 

<p align="center">
  <img width="1431" alt="avg_rent_price_num_rooms.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/charts/avg_rent_price_num_rooms.png?raw=true">
</p>

Again, when we analyze the m2 price by number of rooms, we can clearly see that the price per m2 is much bigger for places with less number of rooms, in other words, the relative cost is much bigger in smaller places or places with less number of rooms.

<p align="center">
  <img width="1431" alt="avg_m2_price_num_rooms.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/charts/avg_m2_price_num_rooms.png?raw=true">
</p>



