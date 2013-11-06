# Apriori algorithm for Hadoop

## Usage

1. create a jar file. The file will be created in the target directory `mvn package`
2. copy that file to the hadoop cluster
3. `hadoop jar fimonhadoop-0.0.1-SNAPSHOT.jar fim.Apriori -D apriori.debug=1  -D apriori.minsup=0.12 -D apriori.baskets=1000  /home/apriori/100 /home/hduser/teszt-1`
