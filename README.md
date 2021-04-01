# BigData_CS644
This Repository contain all Assignments and Project i completed during my Big Data Course at NJIT

# Assignment 1: Set-up AWS EC2 Instance

Take the following steps to set up VM instances through AWS for later use:

Create an Amazon account (if you don't have one yet): http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/AboutAWSAccounts.html
Apply as a student for free credits: http://aws.amazon.com/education/awseducate
Create and launch two basic Amazon EC2 instances using any Linux AMI of your choice
Assign an appropriate security group (with appropriate firewall settings) to allow network traffic between your two instances
Configure "authorized_keys" for an ssh server and "known_hosts" for an ssh client on each VM instance to allow passphraseless ssh login between them

# Assignment 2: Configure Hadoop in standalone Mode and develope a map-reduce Java program to find missing poker card

In one of the VM instances you created in HW2, do the following:

1. Download, install, and run the latest release of Apache Hadoop in a non-distributed or local mode (standalone): http://hadoop.apache.org/releases.html
2. Develop and test a MapReduce-based approach in your Hadoop system to find all the missing Poker cards.

Submission requirements: A zipped file that contains:

1. A text file that contains a random number (<52) of different Poker cards (each card is represented by both its rank and suit)
2. A text file that contains all the missing Poker cards identified by your MapReduce solution
3. The java programs of your MapReduce solution


# Assignment 3: Configure HBase in Standalone Mode and store all missing Poker Cards in HBase

On the VM instance where you installed Hadoop in HW3, do the following:

1. Download, install, and run a stable release of Apache HBase: https://hbase.apache.org/downloads.html
2. Expand your MapReduce program developed in HW3 to i) store all the missing Poker cards in HBase and ii) count the number of records in the generated HBase table.
3. Run the expanded MapReduce program in your Hadoop-HBase system.

Submission requirements: A zipped file that contains

1. A text file that contains a random number (<52) of different Poker cards (each card is represented by both its rank and suit)
2. A text file that contains all the missing Poker cards identified by your expanded MapReduce solution
3. A Screenshot showing that the Hadoop-HBase system is running
4. A Screenshot showing that new records of all the missing Poker cards have been stored in HBase
5. The modified java programs of your MapReduce solution integrated with HBase

# Assignment 4: Configure Apache Hadoop in a pseudo-distributed mode and fully-distributed mode, Develope a MapReduce-based approach in your Hadoop system to compute the relative frequencies of each word, 

![image](https://user-images.githubusercontent.com/57008351/112390859-51978180-8ccd-11eb-8a00-8d6189d418bf.png)

# Project: develop an Oozie workflow to process and analyze a large volume of flight
data

