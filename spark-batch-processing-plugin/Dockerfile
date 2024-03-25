# Use a base image with JDK 8u202 and Scala SDK 2.12.14 installed
FROM openjdk:8

# Set environment variables for Spark and Scala
ENV SPARK_VERSION=3.1.1
ENV SCALA_VERSION=2.12.14

# Install Spark
RUN mkdir -p /opt/spark && \
    curl -sL --retry 3 "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz" | tar -xz -C /opt/spark --strip-components 1

# Set environment variables for Spark and Hadoop home
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /app

COPY target/scala-2.12/spark-batch-processing-plugin_2.12-0.1.0-SNAPSHOT.jar /app/spark-batch-processing-plugin_2.12-0.1.0-SNAPSHOT.jar

ENTRYPOINT ["spark-submit", "--class", "com.berkozmen.MainProcess", "--jars", "/opt/spark/jars/*", "/app/spark-batch-processing-plugin_2.12-0.1.0-SNAPSHOT.jar"]
