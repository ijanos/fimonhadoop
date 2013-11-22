# Apriori algorithm implementations for Hadoop

## About

Various variations of the Apriori frequent itemset miner algorithm for Apache Hadoop.

## Requirements

* Hadoop MRv2 API, available in both Hadoop 1.0 and 2.0 branches.

## Compilation

The packages can be compiled in to a single jar file with the `mvn package` command.

The jar file will be available in the `target` directory.

## Usage

### Single reduce, classic Apriori algorithm

Command line usage:
`hadoop jar fimonhadoop.jar fim.Apriori <parametes> <inputdir>  <outputdir>`

Parameters can be passed to the algorithm after the with -D name=value

* **apriori.debug** set to 1 to enable. It will force every task to run in the
  same JVM on (use the LocalJobRunner) so every output will be delivered to the
  termianal where the job is launched.
* **apriori.minsup**
* **apriori.baskets**

Example:

`hadoop jar fimonhadoop-0.0.1-SNAPSHOT.jar fim.Apriori -D apriori.debug=1  -D apriori.minsup=0.12 -D apriori.baskets=1000  /home/apriori/100 /home/hduser/teszt-1`

### Multiple reduce



### Automation

The automation package can run the algorithm with different paramters and measure the runtimes.
All the paramters must be listed in a csv file.

An example csv file can be found in the `res` directory.

## Scripts

There are helper scripts in the `scripts` directory.

**csvgen.py**: This script generates csv files.

**nodelimiter.sh**: A script to limit the available TaskTrackers with the help of Ambari.

