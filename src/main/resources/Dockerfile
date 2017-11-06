FROM openjdk:8-jre-alpine
COPY build/libs/SparkFinancialAnalysis-1.0-SNAPSHOT.jar /opt/apps/spark
ENTRYPOINT ["java", "-jar", "/opt/apps/spark/SparkFinancialAnalysis-1.0-SNAPSHOT.jar", "com.financial.analysis.spark.SparkFinancialAnalysisMain"]