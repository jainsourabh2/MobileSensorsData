Assuming standalone mode installation.

1. Install
- NodeJS
- Apache Kafka
- Apache Spark

2. Goto Apache Kafka path and start the Zookeeper and Kafka Server
- bin/zookeeper-server-start.sh config/zookeeper.properties
- bin/kafka-server-start.sh config/server.properties

3. Create a Kafka Topic called sensor
- bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sensor

4. Start the Mobile App to send the sensor data.

5. Goto the NodeJS path and start the reciever
- node index1.js

6. Goto Spark Folder and start Spark Streaming
- bin/spark-submit --master local[2] --class com.spark.sprak_kafka.SparkStream <<Path To Jar File>>/spark-kafka-0.0.1-SNAPSHOT.jar localhost:2181 spark_sensor_consumer sensor