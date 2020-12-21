## Project Description

In this project I wanted to perform some analysis on the gigantic Reddit comment archive (~ 300GB Zipped). This dataset is located on SFU's Hadoop cluster and I wrote some PySpark scripts to remotely analyze the data on the cluster. I aimed to write two PySpark pipeline scripts to answer the following two questions:

1) What is the average score in each subreddit?

2) Who is the author of the best comment relative to each subreddit?

## Building the Project

Please note that the data in this repo are just test data, and the actual data dump must be obtained from Reddit. Also, the whole point of this project is so I can get more practice with PySpark so it kind of defeats the purpose if these scripts aren't ran on a Hadoop Cluster.

1) Ensure data is on Hadoop Cluster or on a local drive

2) Run the following commands to run each script:

to answer question 1: `spark-submit reddit_averages.py reddit-3`

to answer question 2: `spark-submit reddit_relative.py reddit-3`
