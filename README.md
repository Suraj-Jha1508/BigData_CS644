# BigData_CS644

Big Data Assignments and Project which i have done in my Masters at NJIT under Course CS644

# Assignment 1:

n one of the VM instances you created in HW2, do the following:

1. Download, install, and run the latest release of Apache Hadoop in a non-distributed or local mode (standalone): http://hadoop.apache.org/releases.html
2. Develop and test a MapReduce-based approach in your Hadoop system to find all the missing Poker cards.

Submission requirements: A zipped file that contains:

1. A text file that contains a random number (<52) of different Poker cards (each card is represented by both its rank and suit)
2. A text file that contains all the missing Poker cards identified by your MapReduce solution
3. The java programs of your MapReduce solution


# Assignment 2:

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

# Assignment 3:
In this assignment, you will explore a set of 100,000 Wikipedia documents: 100KWikiText.txt, in which each line consists of the plain text extracted from an individual Wikipedia document. On the AWS VM instances you created in HW2, do the following:
1.	Configure and run a stable release of Apache Hadoop in a pseudo-distributed mode.
2.	Develop a MapReduce-based approach in your Hadoop system to compute the relative frequencies of each word that occurs in all the documents in 100KWikiText.txt, and output the top 100 word pairs sorted in a decreasing order of relative frequency. Note that the relative frequency (RF) of word B given word A is defined as follows:
 
where count(A,B) is the number of times A and B co-occur in the entire document collection, and count(A) is the number of times A occurs with anything else. Intuitively, given a document collection, the relative frequency captures the proportion of time the word B appears in the same document as A.
3.	Repeat the above steps using at least 2 VM instances in your Hadoop system running in a fully-distributed mode.
Submission requirements:
All the following files must be submitted in a zipped file:
o	A commands.txt text file that lists all the commands you used to run your code and produce the required results in both pseudo and fully distributed modes
o	A top100.txt text file that stores the final results (only the top 100 word pairs sorted in a decreasing order of relative frequency)
o	The source code of your MapReduce solution (including the JAR file)
o	An algorithm.txt text file that describes the algorithm you used to solve the problem
o	A settings.txt text file that describes:
o	i) the input/output format in each Hadoop task, i.e., the keys for the mappers and reducers
o	ii) the Hadoop cluster settings you used, i.e., number of VM instances, number of mappers and reducers, etc.
o	iii) the running time for your MapReduce approach in both pseudo and fully distributed modes
![image](https://user-images.githubusercontent.com/57008351/112390859-51978180-8ccd-11eb-8a00-8d6189d418bf.png)

