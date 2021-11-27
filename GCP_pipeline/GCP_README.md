<h1 align="center">Rent Houses Germany - GCP Pipeline</h1> 

In this version of the Rent Houses Germany project the focus was to learn and get used to Google Cloud Platform tools. 

<p align="center">
  <img width="717" alt="gcp_pipeline" src="https://user-images.githubusercontent.com/71295866/142464782-d7af62c8-866c-4a31-98d8-a6ef20f23130.png">
</p>

The data collection is separated in tree steps:

1. The first script runs localy and collects the number of offers in each city and store it in Cloud Storage
as a CSV file named "offers_qqt_by_city.csv". 

**INSERT DATASET SAMPLE**

2. The second script gets "offers_qqt_by_city.csv" from Cloud Storage and use the number of offers from each city, collect the id numbers from each offer 
and loads the data as a new CSV file called "all_offers_ids.csv" into Cloud Storage.

**INSERT DATASET SAMPLE**

3. The third script uses the offers ids numbers from "all_offers_ids.csv" in order to collect all main informations from each offer and loads it as a third 
CSV file named "all_offers_infos_raw.csv" into Cloud Storage. 

**INSERT DATASET SAMPLE**



<p align="center">
  <img width="1201" alt="data_studio_dashboard" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/Screen%20Shot%202021-11-27%20at%2010.30.34.png?raw=true">
</p>


[German Rent Report - 27.11.21](https://datastudio.google.com/s/lqHHK1S2DRQ)
