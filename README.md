##### Before running script

*connect.py*
1. Suppose you have AWS user with secret key
2. Create IAM role for Redshift
3. Atach policy to that role to read from S3 bucket
4. Create Redshift cluster
5. Open port to receive incoming calls
6. Write ENDPOINT and ROLE_ARN


##### Create staging tables and star schema tables

*create_tables.py*
1. connect to Redshift cluster
2. create schema
3. drop tables if it exists to run multiple times
4. create 2 staging tables as it is from S3 bucket event data and song data
5. create 5 star schema tables: 1 fact and 4 dimension tables, fact and song dimension table are distributed on artists
others distribution is all


##### ETL data from S3 to staging tables and then to analytical tables

*etl.py*
1. connect to Redshift cluster
2. set created schema
3. copy data from S3 bucket to staging tables (suggesting: it will be better to keep staging data and analytical data in different schemas)
4. insert data from staging tables to analytical tables


##### Run some analytical queries

*main.py*


##### Delete cluster

*delete_cluster.py*