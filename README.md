# Data_Lake_with_AWS

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Run Script 

### We need to first set up a Spark Cluster before we run the etl.py script. 

### Create Key Pairs 

1. Login to your AWS account and select the EC2 service.<br/>
2. Select Key Pairs from Network and Security in the right hand side menu.<br/>
3. Click on the Create Key Pair button and give it an appropriate name.<br/>
Note: the private part of the key will be automatically downloaded to your local computer, and make sure the file is stored in a safe place on your local machine. 


### Create Spark Cluster 

1. Login to your AWS account and select the EMR service.<br/>
2. Click on the Create Cluster button.<br/>
3. Give the cluster a name and make sure that Launch Mode: Cluster is selected.<br/>
4. Select hardware configuration m5.xlarge and software configuration release emr-5.27.0 and application Spark. (We can choose any hardware configuration we want)<br/> 
5. Select the number of nodes in your cluster.<br/>
6. In the Security and Access configuration make sure to select the keypair you crerated in the previous section.<br/>
7. Scroll down and click the final Create Cluster button.<br/> 

**After the Spark Cluster is set up, we would be able to run the etl.py pyspark script, but also make sure you have a bucket named "sparkify-data-lake" on your cloud, or whatever you name this bucket and make changes in the etl.py accordingly.**
