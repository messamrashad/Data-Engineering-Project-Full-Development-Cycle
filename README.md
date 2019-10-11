# Data Engineering Nanodegree - Capstone Project

Data Engineering Nanodegree Project - Airbnb Dataset


## Introduction

This is the Capstone project for the Data Engineering Nanodegree Program from Udacity. Udacity grants to us the freedom to choose whether we will use a dataset from suggested datasets provided from Udacity or pick a dataset which matches our interests, and defining the scope by ourselves. In this project, I choose to build a project from scratch, but with a restriction that the chosen dataset must be **BIG & COMPLEX** to have the opportunity to apply what I have learned throughout the program. The dataset I will use is **Airbnb actual data** collected from this [Inside Airbnb](http://insideairbnb.com/get-the-data.html). The dataset contains all info about **(Reviews, Calendars, Listings)** of my favourite city in the whole world **'Amsterdam'**.

## Project Scope

The scope of this project is to read data from **Amazon S3 bucket** and load it in staging tables on **Amazon Redshift**, later process the data in order **to create Fact and Dimension tables**. Finally **some data quality checks** are applied to ensure if any constraints are not applied correctly on the ingested data.

The project follows the following steps:

* Step 1: Gathering a Dataset, Exploring/Assessing the Dataset.
* Step 3: Defining the Data Model.
* Step 4: Run an ETL job to get the data from the S3 bucket and model the data into Fact/Dimensions tables.
* Step 5: Data Quality checks.

Regarding the data model of this project, I will follow the **Snowflake Schema** as some of the relationships are many-to-many which is not supported by **Star Schema**.


## Dataset

The dataset was found on the mentioned source and was compiled by Inside Airbnb Team on date 08 August 2019. This dataset could be used for Data Analytics / Data Engineering purposes or to apply NLP techniques.

#### Source Files
There are four CSV file which i will use in this project:

* Listings.csv with 20677 records </br>
* Calendar.csv with 7547198 records </br>
* Reviews.csv with 481574 records </br>
* Neighbourhoods.csv with 21 records -->  I will Pre-process this file to a JSON file, as mentioned in the **PROJECT RUBRIC** that the dataset must have "At least two data sources/format". </br>


#### Storage
The files (after Exploring/Assessing Phase) will be uploaded to an S3 bucket. The total space of this S3 bucket will be approximately 1 GB.


## Tooling
The used tools in this project were **Apache Airflow**, **Amazon S3 Storage**, **Amazon Redshift**.

- Apache Airflow is an Orchestration Tool
- Amazon S3 for File Storage
- Amazon Redshift for Data Storage

I chose these technologies based on many factors:

1- For Apache Airflow, It's an open-source project, and its community is enormous which is very useful in case of unknown info or methodology. Moreover, It gives the developer the chance of creating new plugins based on his/her needs. </br>

2-They have been illustrated in the Nanodegree Program. </br>


## Data Model

- The final data model includes 6 tables with one Fact Table and 4 dimensions Tables and 1 reference table.

![Data Model](https://user-images.githubusercontent.com/21154169/66619418-2e623a80-ebdd-11e9-86ec-5b3c3147ac87.png)

- This Data model should facilitate the daily reporting tasks for business team regarding who are the users who have the most reviews, 
  where (which neighbourhood) the marketing team should targets in order to increase boooking, and so many info could be extracted from the fact and the dimension tables.


#### Explanation of the datamodel:
Starting with the Dimensions Tables: </br>

- ```DIM_PROPERTIES``` has information about each property and its attributes. </br>

- ```DIM_CALENDARS``` has the already configured calendars by hosts with important attributes such as (ADJUSTED_PRICE, MAXIMUM_NIGHTS,    MINIMUM_NIGHTS) </br>

- ```DIM_HOSTS``` has the essential information for each host. </br>

- ```DIM_REVIEWS``` has information about each review, the reviewer, and the date of the review. </br>

The Fact Table: </br>
- ```Airbnb_Amst_Facts``` has information about hosts, their calendars and their lists, and the most important measures are the **NUMBER_OF_REVIEWS**, **REVIEWS_RATING**, **POTIENTIAL_EARNINGS** and **POTIENTIAL_AVALIABLE_NIGHTS**.

The Reference Table: </br>
- ```REF_NEIGHBOURHOODS``` has only two columns: **NEIGHBOURHOODS_ID** and **NEIGHBOURHOODS_NAME**. The purpose of this reference table is to be populated "in the future" within the Fact Table which will help the marketing/business team to easily select which neighbourhoods they should target frequently with ads/offers because a lot of travellers want to get accommodated in it.



## Data Dictionary

```DIM_REVIEWS```:

|         FIELD        | TYPE |  PK | FK |
|:--------------------:|:----:|:---:|:--:|
|       REVIEW_ID      |  INT | YES |    |
|      REVIEWER_ID     |  INT |     |    |
|      REVIEW_DATE     | DATE |     |    |
| REVIEW_SCORES_RATING |  FLOAT |     |   |
| COMMENT              |  INT |     |    |
|        LISTING_ID    | INT |     | YES|


```DIM_PROPERTIES```:

|        FIELD        |   TYPE  |  PK | FK |
|:-------------------:|:-------:|:---:|:--:|
|       LISTING_ID       |   LONG  | YES |    |
|      ROOM_TYPE      | VARCHAR |     |    |
|    PROPERTY_TYPE    | VARCHAR |     |    |
|   GUSTES_INCLUDED   |   INT   |     |    |
| CANCELLATION_POLICY |   INT   |     |    |
|       HOST_ID       |   INT  |     | YES|
|   NEIGHBOURHOOD_ID  |   INT   |     | YES|


```DIM_CALENDARS```:

|      FIELD     |   TYPE  |  PK |  FK |
|:--------------:|:-------:|:---:|:---:|
|   CALENDAR_ID  |   INT   | YES |     |
|      DATE      |   DATE  |     |     |
|    AVAILABLE   | VARCHAR |     |     |
| ADJUSTED_PRICE |   INT   |     |     |
| MAXIMUM_NIGHTS |   INT   |     |     |
| MINIMUM_NIGHTS |   INT   |     |     |
|     LIST_ID    |   INT  |     | YES |


```DIM_HOSTS```:

|           FIELD           |   TYPE  |  PK | FK |
|:-------------------------:|:-------:|:---:|:--:|
|          HOST_ID          |   INT  | YES |    |
|         HOST_NAME         | VARCHAR |     |    |
|          HOST_URL         | VARCHAR |     |    |
|         HOST_SINCE        |   DATE  |     |    |
|     HOST_RESPONCE_TIME    | VARCHAR |     |    |
|        IS_SUPERHOST       | VARCHAR |     |    |
| HOST_TOTAL_LISTINGS_COUNT |   INT   |     |    |
| LISTING_ID                |   INT   |     |    |


```REF_NEIGHBOURHOODS```:


|        FIELD       |   TYPE  |  PK | FK |
|:------------------:|:-------:|:---:|:--:|
|  NEIGHBOURHOOD_ID  |   INT   | YES |    |
| NEIGHBOURHOOD_NAME | VARCHAR |     |    |


```Airbnb_Amst_Facts```:

|            FIELD            | TYPE |  PK |  FK |
|:---------------------------:|:----:|:---:|:---:|
|           FACT_ID           |  INT | YES |     |
|           HOST_ID           | LONG |     | YES |
| LIST_ID                     | LONG |     | YES |
| NUMBER_OF_REVIEWS           | INT  |     |     |
| REVIEWS_RATING              | INT  |     |     |


## Scenarios
The **PROJECT RUBRIC** has mentioned that the submitted project must address the following scenarios:

**1- The data was increased by 100x.**
- This scenario would not be considered as a major issue, because of the used tools and technologies in this project, for example, Amazon tools either Redshift or S3 are commonly known as reliable and can deal with VERY large data. Thus, in the case of this scenario, I expect that the size of the S3 bucket would be increased and according to that, the tables in Redshift would grow too.

**2- The pipelines would be run daily by 7 am every day.**
- This scenario will be just correctly fit the tools and technologies used in this project. The Development/Operations team will easily set the schedule interval to be daily at 7 am and it's done.

**3- The database needed to be accessed by 100+ people.**
- Because as mentioned before, Amazon web services are commonly known for its stability and scalability features. So, it would not be considered as an issue or even needed major changes in the platform to be done properly.


## Data Pipeline

#### Exploring and Assessing the data:
- I followed a DATA WRANGLING approach to assessing the dataset and clean it as possible to benefit from this rich dataset with being distracted with unimportant fields. </br>

- You can find all the applied steps in the notebook "Exploring-Assessing Airbnb Dataset.ipynb".

#### My ETL pipeline includes 18 tasks:

- ```START_OPERATOR```,```MID_OPERATOR```, ```END_OPERATOR``` are just dummy tasks, starting and ensuring all tasks are syncronized with each other and finishing the execution. </br>

- ```Create_Reviews_STG_Table```, ```Create_Calendars_STG_Table```, ```Create_Hosts_STG_Table``` and ```Create_Neighbourhoods_STG_Table``` are tasks responsible for creating STG tables on the Redshift cluster. </br>

- ```Stage_reviews```, ```Stage_listings```, ```Stage_calendars``` and ```Stage_neighbourhoods```  are tasks responsible for copying the date from S3 to Redshift. </br>

- ```Create_DIM_HOSTS_Table```, ```Create_DIM_PROPERTIES_Table```, ```Create_DIM_REVIEWS_Table``` and ```Create_DIM_CALENDARS_Table``` are tasks responsible for creating **Dimensions** tables on the Redshift cluster. </br>

- ```LOAD_DIM_HOSTS_TABLE```, ```LOAD_DIM_PROPERTIES_TABLE```, ```LOAD_DIM_REVIEWS_TABLE``` and ```LOAD_DIM_CALENDARS_TABLE```  are tasks responsible for copying the date from STG tables to Redshift. Knowing that, there are many conditions to filter unimportant/non-common sense records. </br>

- ```Create_DIM_HOSTS_Table```, ```Create_DIM_PROPERTIES_Table```, ```Create_DIM_REVIEWS_Table``` and ```Create_DIM_CALENDARS_Table``` are tasks responsible for creating **Dimensions** tables on the Redshift cluster. </br>

- ```Create_Load_FACT_AIRBNB_AMST_TABLE``` task is responsible for creating the **FACT** table and start to aggregate the events from the dimensions tables to build a query-based fact tables for business users. </br>


## Data Processing
I used SQL statements to process the data from S3 bucket to already created tables on Redshift. For each task, there is an equivilant SQL statement does the data ingestion process smoothly with consideration of the performance with such big datasets.

Here is an example on how i ingest the data from STG tables into one of DIMENSION tables ```DIM_CALENDARS``` : </br>

```sql
INSERT INTO DIM_CALENDARS 
        (CALENDAR_DATE, AVAILABLE, ADJUSTED_PRICE, MINIMUM_NIGHTS, MAXIMUM_NIGHTS, LISTING_ID)
        SELECT DISTINCT CALENDAR_DATE, AVAILABLE, ADJUSTED_PRICE, MINIMUM_NIGHTS, MAXIMUM_NIGHTS, LISTING_ID
        FROM STG_CALENDARS
```
## How to run the project

1- Ensure that you have an Airflow instance is up and running. </br>

2- Copy all the content of dags and plugins directories to your Airflow work environment.</br>

3- On your Airflow GUI, under Admin drop-down list, select connections and create two connections with the following inputs:</br>

- ```Conn_id```: aws_credentials , ```conn Type```: **Amazon Web Services**, ```Login```: "YOUR ACCESS_KEY_ID", ```PASSWORD```: "Secret Access Key" <br/>

- ```Conn_id```: redshift , ```conn Type```: **Postgres**, ```Login```: "YOUR REDSHIFT CLUSTER"(WITHOUT THE PORT) , ```PASSWORD```: "YOUR REDSHIFT CLUSTER PASSWORD, ```PORT```: "YOUR REDSHIFT CLUSTER PORT <br/>

4- Activate the DAG and run it.

