# Rent Houses - Germany


This is a student project with the sole purpose of practicing new tools and concepts. The initial idea is to build a complete ELT/ETL process and display the data as information. There are no predefined tools, no specific architecture, just a student using the most recently learned tools (there is also the issue of time, always little time to work on the project). Sometimes it will be an extraction, transform and load (ETL) process, sometimes an extractions, load and transform (ELT) or even a simple extraction and load (EL). Sometimes it will be done locally, using AWS, GCP, Streamlit or Heroku, it depends.
The basis of the project is to extract information about rental offers in some German cities, store it, work with it and display it, as showed in the follow diagram.

<p align="center">
  <img width="459" alt="main_dataflow" src="https://user-images.githubusercontent.com/71295866/141155451-a9b1dff0-3adf-448f-af5c-f54c1de21b62.png">
</p>


## 1st - Data Extraction.

Data source: https://www.immonet.de/

The first step of the project was to build the extration part of the data pipeline. three python scripts were written to perform the task. It could be done in a simplier way, but the real intention here was to practice and try some new packeges, using python to connect with a postgres database, load data into the database eng get data from the database. The first script only gets the quantity of rent offers for 11 german cities and load the data into the database in a cumulative way (append) to build a "historical table".

<p align="center">
  <img width="449" alt="Screen Shot 2021-11-10 at 20 59 18" src="https://user-images.githubusercontent.com/71295866/141184872-59eece7e-fd4d-456c-8366-66dbbe9928e1.png">
</p>


The second script get the previous table from the database and use it as base to run each offer for each city and extract the "offer id". All ids, from each offer, are stored in a new table and load into the database.

<p align="center">
  <img width="530" alt="Screen Shot 2021-11-10 at 21 10 55" src="https://user-images.githubusercontent.com/71295866/141186226-b9948549-9489-4aa4-9984-80ed31b27f8b.png">
</p> 

The third script uses the previous one (use the offer ids) to extract all main information from each offer from each city. All the information is extracted in a raw format. Actualy, it gets a json object and store it into the database.

<p align="center">
  <img width="1431" alt="Screen Shot 2021-11-10 at 21 22 05" src="https://user-images.githubusercontent.com/71295866/141187696-67002d2e-f1fb-409e-97de-e66726b4fddd.png">
</p>

From here we have the base data for further work.

