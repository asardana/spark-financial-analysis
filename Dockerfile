FROM openjdk:8-jre-alpine
MAINTAINER asardana.com
RUN mkdir -p /opt/apps/spark
COPY build/libs/SparkFinancialAnalysis-1.0-SNAPSHOT-all.jar /opt/apps/spark
RUN ls -ltr
RUN mkdir -p /bigdata
COPY src/main/resources/LoanStats_2017Q2.csv /bigdata/LoanStats_2017Q2.csv
RUN cd /opt/apps/spark && ls -ltr
ENTRYPOINT ["java", "-jar", "/opt/apps/spark/SparkFinancialAnalysis-1.0-SNAPSHOT-all.jar"]