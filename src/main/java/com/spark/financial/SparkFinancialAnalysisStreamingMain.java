package com.spark.financial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by Aman on 10/15/2017.
 */
public class SparkFinancialAnalysisStreamingMain {

    public static void main(String args[]) {

        // Set Spark Configuration
        SparkConf sparkConf = new SparkConf().setAppName("spark-financial-analysis").setMaster("local[*]");
        sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

        SparkFinancialAnalysisStreamingMain sparkStreamingFinMain = new SparkFinancialAnalysisStreamingMain();

        sparkStreamingFinMain.aggregateOnStreamingData(sparkConf);


    }
    /**
     * Reads the Streaming data from the file system
     * @param conf
     */
    public void aggregateOnStreamingData(SparkConf conf){

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaDStream<String> loadStatsStream = streamingContext.textFileStream("/tmp/streaming");

        // Convert the streams of JavaRDD of Strings to JavaRDD of Java Beans
        loadStatsStream.foreachRDD((JavaRDD<String> loanStatsTextRDD) -> {

            JavaRDD<LoanDataRecord> loanDataRecordRDD =  loanStatsTextRDD.map((line) -> {

                LoanDataRecord loanDataRecord = new LoanDataRecord();

                if(!(line.isEmpty() || line.contains("member_id") || line.contains("Total amount funded in policy code"))){

                    // Few records have emp_title with comma separated values resulting in records getting rejected. Cleaning the data before creating Dataset
                    String updatedLine = line.replace(", ", "|").replaceAll("[a-z],", "");

                    String loanRecordSplits[] = updatedLine.split(",\"");

                    loanDataRecord.setLoanAmt(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[2]));
                    loanDataRecord.setFundedAmt(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[3]));
                    loanDataRecord.setTerm(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[5]));
                    loanDataRecord.setIntRate(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[6]));
                    loanDataRecord.setGrade(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[8]));
                    loanDataRecord.setHomeOwnership(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[12]));
                    loanDataRecord.setAnnualIncome(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[13]));
                    loanDataRecord.setAddressState(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[23]));
                    loanDataRecord.setPolicyCode(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[51]));
                }else {
                    System.out.println("Invalid Record line " + line);
                }
                return loanDataRecord;
            }).filter(record -> record.getFundedAmt()!=null);

            SparkSession session = SparkSession.builder().config(conf).getOrCreate();

            Dataset<Row> loanStatFullDataset = session.createDataFrame(loanDataRecordRDD, LoanDataRecord.class);

            loanStatFullDataset.show();

            if(!loanStatFullDataset.rdd().isEmpty()) {

                loanStatFullDataset.write().mode(SaveMode.Append).parquet("/tmp/loanStatFullDataset");


                System.out.println("Streaming Financial Statistics Record count " + loanStatFullDataset.count());

                // Calculate the total funding amount for a given State
                List<String> fundedAmountsForState = loanStatFullDataset.javaRDD().filter(row -> "IL".equalsIgnoreCase(row.getString(0))).map(row -> row.getString(2)).collect();

                String totalFundedAmountForState = fundedAmountsForState.stream().reduce((x,y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

                System.out.println("Total Amount funded by Lending Club in IL State : $" + new BigDecimal(totalFundedAmountForState).toPlainString());
            }

        });

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
