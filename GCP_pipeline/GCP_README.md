<h1 align="center">Rent Houses Germany - GCP Pipeline</h1>  

<p align="center">
  <img width="717" alt="gcp_pipeline" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/Screen%20Shot%202021-11-27%20at%2013.57.08.png?raw=true">
</p>

### Project:

The goal of the project is to extract data about house rentals in Germany, store, process and analyze it using GCP tools. The main goal here is to practice and get used to the GCP environment.

**Main Tools:**


  <table align="center">
       <tbody>
         <tr valign="top">
            <td width="25%" align="center">
              <span>Python</span><br><br>
              <img height="64px" src="https://cdn.svgporn.com/logos/python.svg">
            </td>
            <td width="25%" align="center">
              <span>Cloud Storage</span><br><br>
              <img height="64px" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/cloud_storage.png?raw=true">
            </td>
            <td width="25%" align="center">
              <span>BigQuery</span><br><br>
              <img height="64px" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/bigquery.png?raw=true">
            </td>
            <td width="25%" align="center">
              <span>Dataprep</span><br><br>
              <img height="64px" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/dataprep.png?raw=true">
            </td>
          </tr>
          <tr valign="top">
            <td width="25%" align="center">
              <span>Data Studio</span><br><br>
              <img height="64px" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/data_studio.png?raw=true">
            </td>
            <td width="25%" align="center">
              <span>Looker</span><br><br>
              <img height="64px" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/looker.png?raw=true">
            </td>
            <td width="25%" align="center">
              <span>Crontab</span><br><br>
              <img height="64px" src="https://www.pc-freak.net/images/linux-unix-list-all-crontab-users-and-scripts.png">
            </td>
            <td width="25%" align="center">
              <span>Bash</span><br><br>
              <img height="64px" src="https://styles.redditmedia.com/t5_2qh2d/styles/communityIcon_xagsn9nsaih61.png?width=256&s=1e4cf3a17c94aecf9c127cef47bb259162283a38">
          </tr>
        </tbody>
      </table>

### Data Extraction and Storage:

Source: https://www.immonet.de/

- The data extraction is done in 3 steps where first the quantity of offers for each city is collected, them the ID's for each offers and finaly the raw information about each rent offer is extracted.

- The first script is responsible to scrape the number of offers in each city and save the information as a CSV file in Cloud Storage. The second script gets the previous CSV file from Cloud Storage and uses it to scrape all ID's from each offers in each city and load the information back to Cloud Storage as a new CSV file. The third script gets the rent offer's ID info from Cloud Storage and perform a web-scraper to collect all information for each ID and save it back to Cloud Storage, again as a CSV file containing all raw infos about the offers.

### Data Preprocessing.

As the last CSV file contains all the RAW information about each offer grouped in only two columns, a preprocessing step is needed. The preprocessor script gets the CSV file with the raw information from Cloud Storage, separates the data into the appropriate columns already performing some cleaning like excluding not needed characters.

```all_offers_infos_raw.csv```:





<p align="center">
  <img width="1201" alt="data_studio_dashboard" src="https://github.com/felipedmnq/rent-houses--germany/blob/master/GCP_pipeline/images/Screen%20Shot%202021-11-27%20at%2010.30.34.png?raw=true">
</p>


[German Rent Report - 27.11.21](https://datastudio.google.com/s/lqHHK1S2DRQ)
