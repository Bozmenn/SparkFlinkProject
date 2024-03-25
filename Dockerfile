FROM openjdk:8-jre-alpine

WORKDIR /app

COPY DataProducer.jar .

CMD ["java", "-jar", "DataProducer.jar", "com.trendyol.dataeng.bootcamp.DataProducer"]
