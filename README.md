# AWS-Data-Warehouse
- Project delivered in June 2023
- Repository includes six files:
    * A `SDK_create.ipynb` notebook that contains a series of scripts to create an IAM role and Redshift cluster
    * A `sql_queries.py` script that contains variables with SQL statements in string formats
    * A `create_tables.py` script that drops existing tables and re-creates new tables, per Sparkify's specifications
    * An `etl.py` script that extracts JSON data from S3 buckets and loads them to a Redshift cluster
    * An `dhw.cfg` configuration file that contains the parameters for the Redshift cluster, the S3 locations of the log and songs data, as well as the IAM role
    * A `SDK_destroy.ipynb` notebook that contains a series of scripts to remove the IAM role and Redshift cluster created for the project

## Problem Statement
- A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Usage

### Configuration

Set up a config file `dwh.cfg` that uses the following schema. To use the SDK notebooks, you'll have to create access keys for AWS. If these keys are unavailable, you can manually create these assets via the AWS Management Console. Once these assets are available, complete the information for your Redshift cluster and IAM-Role that can manage your cluster and read S3 buckets. 

Below, I have shared example inputs or provided descriptions.
```
[AWS]
KEY = #user-input
*The AWS Access Key ID is a unique identifier that is used to identify the AWS account associated with an API request.
It is a 20-character alphanumeric string, similar to "AKIAIOSFODNN7EXAMPLE".
The Access Key ID is used to identify the user or application making API calls to AWS services.*

SECRET = #user-input
*The Secret Access Key is a 40-character alphanumeric string that is paired with the Access Key ID.
It is used to sign requests made to AWS services for authentication purposes.
The Secret Access Key should be kept confidential and should not be shared or exposed publicly*

[DWH]
DWH_CLUSTER_TYPE = multi-node
DWH_NUM_NODES = 4
DWH_NODE_TYPE = dc2.large
DWH_CLUSTER_IDENTIFIER = projectCluster
DWH_DB = #user-input
DWH_DB_USER = #user-input
DWH_DB_PASSWORD = #user-input
DWH_PORT = 5439
DWH_IAM_ROLE_NAME = #user-input

[S3]
LOG_DATA= s3://udacity-dend/log_data
LOG_JSONPATH= s3://udacity-dend/log_json_path.json
SONG_DATA= s3://udacity-dend/song_data

*Note: complete the SDK_create notebook to generate the following inputs*
[IAM_ROLE]
ARN = #user-input
*An Amazon Resource Name (ARN) is a unique identifier for resources within Amazon Web Services (AWS).
It is a string of characters that follows a specific format and is used to identify and access various AWS resources.*

[CLUSTER]
HOST = #user-input
*The endpoint of a Redshift cluster typically follows the format:
<cluster-identifier>.<random-characters>.<region>.redshift.amazonaws.com*

DB_NAME = #user-input
DB_USER = #user-input
DB_PASSWORD = #user-input
DB_PORT_2 = 5439
```

### ETL Pipeline Instructions

Once your Redshift cluster is created, ensure you have the appropriate virtual environment set-up and activated. Then, run the following commands in the terminal.

#### To create the tables in Redshift cluster

```bash
python create_tables.py
```

#### To load the data into the Redshift cluster

```bash
python etl.py
```

## Solution Discussion
Sparkify, a music streaming startup, has been amassing significant amounts of user activity and song metadata. This information, currently stored as JSON logs in an S3 bucket, is immensely valuable for gaining insights into user behavior and preferences, which in turn can drive key business decisions.

1. Structured Data: Raw logs and metadata often contain a lot of extraneous information that is not immediately useful for analysis. By extracting the data from S3 and staging in Redshift, we can start structuring this data and filter out the noise.

2. Insights and Analytics: Sparkify's ultimate goal is to understand user behavior. The analytics team will be able to query these tables to answer questions such as:
    - What songs are users listening to the most?
    - What artists are trending?
    - How are different user demographics interacting with the app?
    - What are the peak times for usage?

3. Scalability and Performance: Redshift is designed for high-performance analysis and can handle large volumes of data. As Sparkify grows, the data volume will grow as well. Redshift can scale quickly to accommodate this growth, ensuring the data engineering and analytics operations can keep up with the expanding user base and data size.

Overall, the goal of this database solution should be to optimize Sparkify's data for analysis, allowing the company to derive insights that could drive business growth, user engagement, and user experience improvements.

### Database Schema Design:
I've proposed a star schema for the database design, centered around the songplays table as the fact table. This design decision was based on the needs of the analysis team and the nature of the questions they'll be asking.

The fact table contains the records in a log of song plays, which is the core focus of Sparkify's analysis efforts, and keys to the four dimension tables: users, songs, artists, and time. By organizing our data this way, we ensure that:

- It's simple to understand: Even those with a modest understanding of the database will be able to make sense of the model and write effective queries.
- It's optimized for aggregation: The star schema is excellent for handling common analytical operations, like COUNT, SUM, AVG, MIN, MAX, etc., which are central to your business queries.
- It's fast: The star schema reduces the number of joins required to answer a query, which greatly improves query performance.

The four dimension tables have been chosen to provide additional context to the facts recorded in songplays.

### ETL Pipeline:
Our ETL pipeline has been designed to automate the data flow from raw logs in the S3 bucket to structured, queryable tables in Redshift. The pipeline is constructed as follows:

1. Extract data from Sparkify's S3 bucket: This is the source of Sparkify's raw JSON logs for user activity and song metadata.
2. Stage the extracted data in Redshift: The data is loaded into staging tables in Redshift, enabling fast, SQL-based analysis. Staging the data in Redshift also provides a buffer to Sparkify's raw data and adds an extra layer of security.
3. Transform and Load data into dimensional and fact tables: This involves transforming the data into a suitable format for the star schema and then loading it into our fact and dimension tables. Redshift's columnar storage and massively parallel processing (MPP) architecture ensure this process is fast, even with a growing volume of data.

This ETL pipeline is effective because it automates the transformation of raw, semi-structured data into a structured, analyzable format. Furthermore, it's designed with scalability in mind, ensuring Sparkify can continue to obtain the same high-quality insights as Sparkify's user base grows.
