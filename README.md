# Twitter_Streaming

Instructions:
1. Assembly.
2. run the jar file on console with command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class Twitter /Users/qigao/Desktop/Assignment3-1-n/target/scala-2.11/kafka-assembly-0.1.jar args(0) args(1), which args(0) is keyword you want to search, and args(1) is your consumer topic of kafka
3. run ELK
4. see the graph on Kibana.