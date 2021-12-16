## Postgres Database.

This solution was build to extract the needed information from a website (https://www.immonet.de/) and store it localy in a postgres database.

<p align="center">
  <img width="630" alt="Screen Shot 2021-11-11 at 17 46 57" src="https://user-images.githubusercontent.com/71295866/141341515-b883923e-9843-4572-84a2-00856fc22d9e.png">
</p>


The first step was building the extration part of the data pipeline. Three python scripts were written to perform the task. 
It could be done in a simplier way, but the real intention here was to practice and try some new packeges using python to connect with a postgres database, 
load data into the database eng get data from the database. 

The first script (```get_offers_by_city.py```) gets the quantity of rent offers for 11 german cities and load the data 
into a database in a cumulative way (append) to build a "historical table".



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

The third script (```get_offers_infos.py```) uses the previous one (use offer ids) to extract all main information from each offer from each city. 
All the information is extracted in a raw format, all the main information comes in as a list to be tranformed and cleaned later. 

<p align="center">
  <img width="1431" alt="Screen Shot 2021-11-10 at 21 22 05" src="https://user-images.githubusercontent.com/71295866/141187696-67002d2e-f1fb-409e-97de-e66726b4fddd.png">
</p>

## Data Analasysis

### Dataframe Description

* **offer_id** - unique id for each offer.
* **extraction_date** - extraction datetime.
* **lat** - latitude.
* **lng** - longitude.
* **city** - city.
* **area_m2** - area in squared meters.
* **furnished** - whether is furnished or unfurnished.
* **zip_code** - zip code.
* **main_category** - main category of the property.
* **rooms** - ho many rooms the property have.
* **build_year** - year of construction of the property.
* **state** - state.
* **sub_category** - sub-category of the property. 
* **balcony** - whether is has a balcony or not.
* **heat_type** - which time of head system.
* **offer_title** - the title of the property ad.
* **kitchen** - whether it has a kitchen installed or not.
* **rent_price** - rent price in Euros.
* **garden** - whether it has a garden or not.

### Descriptive Analysis

|    | attributes   |    min |             max |           range |             avg |        median |             std |    skewness |   kurtosis |
|---:|:-------------|-------:|----------------:|----------------:|----------------:|--------------:|----------------:|------------:|-----------:|
|  1 | area_m2      |     15 |   400           |   385           |    68.9725      |  63.01        |    33.8502      |   2.04937   |   8.77325  |
|  2 | rooms        |      1 |    11           |    10           |     2.28184     |   2           |     0.976887    |   0.961887  |   2.65717  |
|  3 | build_year   |   1740 |  2021           |   281           |  1978.87        | nan           |    40.9994      |  -0.78613   |  -0.162868 |
|  4 | balcony      |      0 |     1           |     1           |     0.419285    |   0           |     0.493442    |   0.32723   |  -1.89355  |
|  6 | kitchen      |      0 |     1           |     1           |     0.506451    |   1           |     0.499958    |  -0.0258107 |  -2        |
|  7 | rent_price   |    200 |  8600           |  8400           |  1023.42        | 850           |   719.659       |   3.08872   |  17.7604   |
| 18 | garden       |      0 |     1           |     1           |     0.142077    |   0           |     0.34913     |   2.05088   |   2.20684  |

From this statistic description we can see that our two main columns (area_m2 and rent_price) are highly skewed and have high kurtosis values. 
The positive skewness tells that for rent prices and areas we have a higher frequency of values below the average than above the average. 
The positive and high kurtosis tell us that our data are heavy-tailed and suggest the presence of outliers.

### Exploratory Data Analysis

The first step was to check the characteristics and distributions of rent price.

<p align="center">
  <img width="1431" alt="rent_price_desc.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/charts/rent_price_desc.png?raw=true">
</p>

Here we can see the presence of some outliers, the positive skewness with a tail directed to higher prices and the scatter plot shows us the exactly distribution of the rent price.

The next chart shows us The rent price distribution for each city.

<p align="center">
  <img width="1431" alt="rent_price_desc.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/images/boxplot_rent_by_city.png?raw=true">
</p>

### Some Hypothesis

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

Wordcloud text here

<p align="center">
  <img width="1431" alt="avg_m2_price_num_rooms.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/charts/wordcloud.png?raw=true">
</p>


Final map here

<p align="center">
  <img width="1431" alt="avg_m2_price_num_rooms.png" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/images/2021-12-16%2005.50.23.gif?raw=true">
</p>


