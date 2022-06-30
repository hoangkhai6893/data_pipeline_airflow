# Project: Data Pipelines with Airflow
## Introduction: 
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
## Project Overview

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

In this project, we have 2 different ways to use Airflow:
1. Airflow webserver support by Udacity
2. Airflow localhost with Docker Container.

For project is easier, we can use Airflow webserver but if you want to know how Airflow system is work , you can use docker container to deploy Airflow.
## Prerequisites
- Install Docker and Docker Compose
- Install Python and python pip
- Create an IAM User in AWS and  a redshift cluster in AWS
## Datasets
This project need to  working with two datasets :
- Log data: s3://udacity-dend/log_data
- Song data: s3://udacity-dend/song_data
  
## Project Template
To get started with the project:

1. On the project workspace page after the project instructions, you'll find the project template. You can work on your project and submit your work through this workspace. Alternatively, you can download the project template package and put the contents of the package in their respective folders in your local Airflow installation. The project template package contains three major components for the project:
   - The dag template has all the imports and task templates in place, but the task dependencies have not been set
   - The operators folder with operator templates
   - A helper class for the SQL transformations
2. With these template files, you should be able see the new DAG in the Airflow UI. The graph view should look like this: