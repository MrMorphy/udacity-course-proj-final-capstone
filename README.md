# udacity-course-proj-final-capstone 🌎:
Closing Project "Capstone" at EMR Cluster &amp; Spark on Hadoop (AWS) 24.04.2023 for Udacity Data Engineer Nanodegree.

## Summary ℹ️
* [Introduction](#introduction)
* [Data Schema](#data-schema)
* [Files list](#files-list)
* [Conclusion](#Conclusion)
* [Getting Started](#getting-started)


## Introduction ✏️:

I am running my Capstone project with public data from "Inside Airbnb", in specific from Mexico City. 
All the files required are downloadable from [https://insideairbnb.com](http://insideairbnb.com/get-the-data/). 

![mexico-city](images/IMAGE-Airbnb-Mexico-City-ZoomedIn.jpg)

I select to setup my project with `Spark` (an EMR at Amazon Web Services), a `JUPITER Workbook` (steps and results are directly documented) and for a direct execution of the ETL an `etl.py` Python file will be available.
At the execution of the JUPITER Workbook it will be `pyspark` used, instead of using Redshift and Airflow.
Looking about the cost factor Redshift is expensive, for another we can load the cleaned data from s3 with Spark easily and do big data analytics. 
The Airbnb dataset will be updated monthly, so it may not be neccessary to build a pipeline to it. 

[top](#summary)

## Data Schema ✨:

### Schema design and ETL pipeline

The star schema does consist of 1 *Fact* table (booking) and 4 *Dimension* tables (calendar, listing, neighbourhoods and reviews). The 4 source files in CSV format of **calendar**, **listings**, **neighbourhooods** as well of **reviews** are on the (local:) Udacity Workspace or (online:) at the S3 on AWS uploaded.

![data-model](images/IMAGE-Data-Model.jpg)


## Datasets used
The datasets used are retrieved from the s3 bucket and are in CSV format. There are four datasets namely `calendar`, `listings`, `neighbourhoods` and `reviews`. 
<font color="red">Mas mas y otro poquito mas...</font>


## Motivation ✏️:
The intention of this project is at the end, have combined tables as source for a 'Self Service Report'-Dashboard done with PowerBI. 


## Files list 📎:

Use this files for creation of the database Sparkifydb withhin the JUPITER Workplace with the required tables.  

Following is a list or required files to execute the project: 
* **aws-access.cfg** - configuration properties for (AWS) remote execution
* **etl.py** - main python script with `function_A()`, `process_data_calendar()` and `process_data_listings()`, <font color="red">y algo mas...</font>
* **README.md** - this file as project description
* **Capstone-Notebook.ipynb** - Jupyter Workbook: Data Exploration as well an easy execution of ALL steps and scripts

## Conclusion 🏁:



[top](#summary)

## Getting Started 📖:

### Prerequisites

- AWS Account
- Set your AWS access and secret key in the config file `aws-access.cfg`  
  
```
AWS_ACCESS_KEY_ID = <your aws key>
AWS_SECRET_ACCESS_KEY = <your aws secret>
```

## Setup an EMR on AWS - How to run this project on AWS EMR?

### Create a Data Lake with Spark and AWS EMR

To create an Elastic Map Reduce (EMR) data lake on AWS, use the following steps:

1. Create a ssh key-pair to securely connect to the EMR cluster that we are going to create. Go to your EC2 dashboard and click on the key-pairs. Create a new one, you will get a .pem file that you need to use to securely connect to the cluster.

2. Next, we will create an EMR cluster with the following configuration. Here EMR operates Spark in YARN cluster mode and comes with Hadoop Distributed file system. We will use a 4 cluster node with 1 master and 3 worker nodes.

3. We will be using the default security group and the default EMR role `EMR_DefaultRole` for our purposes. Ensure that default security group associated with Master node has SSH inbound rule open, otherwise you won't be able to ssh. 

![emr-setup](images/IMAGE-EMR-Setup.jpg)


### Terminal commands 

* Execute the ETL pipeline script by running:
    ```
    $ python etl.py
    ```


<br>


[top](#summary)


## Author 🕵️‍♂️:

Best regards  
UDACITY [Data-Engineering]  
Student "MrMorphy" [GitHub Profile](https://github.com/MrMorphy)

GitHub Project  
https://github.com/MrMorphy/udacity-course-proj-final-capstone/blob/main/README.md
