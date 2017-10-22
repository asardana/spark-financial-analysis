package com.financial.analysis.spark;

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

        // Perform aggregation on the Streaming Data
        sparkStreamingFinMain.aggregateOnStreamingData(sparkConf);


    }
    /**
     * Reads the Streaming data from the file system and converts to a Dataset for running Spark SQL queries
     * @param conf
     */
    public void aggregateOnStreamingData(SparkConf conf){

        // Creates Java Streaming Context with batch size of 10 seconds
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaDStream<String> loanStatsStream = streamingContext.textFileStream("/bigdata/streaming");

        // Convert the streams of JavaRDD of Strings to JavaRDD of Java Beans
        loanStatsStream.foreachRDD((JavaRDD<String> loanStatsTextRDD) -> {

           // Flag to indicate if at least some records have been received and aggregated
            boolean isAggregatedDataAvailable = false;

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

            // Create Spark Session
            SparkSession session = SparkSession.builder().config(conf).getOrCreate();

            // Create a Dataset of Rows (Dataframe) from the RDD of LoanDataRecord
            Dataset<Row> loanStatFullDataset = session.createDataFrame(loanDataRecordRDD, LoanDataRecord.class);

            loanStatFullDataset.show();

            // Is data available in the Stream, save the data in Parquet format for future use. Also, calculate the total
            // funding amount for a given state

            if(!loanStatFullDataset.rdd().isEmpty()) {

                loanStatFullDataset.write().mode(SaveMode.Append).parquet("/bigdata/loanStatFullDataset");

                isAggregatedDataAvailable = true;

                System.out.println("Streaming Financial Statistics Record count in current file " + loanStatFullDataset.count());

                // Calculate the total funding amount for a given State in current file of streaming records
                List<String> fundedAmountsForState = loanStatFullDataset.javaRDD().filter(row -> "IL".equalsIgnoreCase(row.getString(0))).map(row -> row.getString(2)).collect();

                String totalFundedAmountForState = fundedAmountsForState.stream().reduce((x,y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

                System.out.println("Total Amount funded by Lending Club in IL State in current file : $" + new BigDecimal(totalFundedAmountForState).toPlainString());
            }

            try{

                // Check if the aggregated data is available in the parquet file
                if(isAggregatedDataAvailable) {

                    // Read the aggregated data from the parquet File for batch records received so far
                    Dataset<Row> aggregatedLoanDataParquet = session.read().parquet("/bigdata/loanStatFullDataset");

                    if (!aggregatedLoanDataParquet.rdd().isEmpty()) {

                        System.out.println("Streaming Financial Statistics aggregated Record count " + aggregatedLoanDataParquet.count());

                        // Calculate the total funding amount for a given State in current file of streaming records
                        List<String> fundedAmountsForStateAgg = aggregatedLoanDataParquet.javaRDD().filter(row -> "IL".equalsIgnoreCase(row.getString(0))).map(row -> row.getString(2)).collect();

                        String totalFundedAmountForStateAgg = fundedAmountsForStateAgg.stream().reduce((x, y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

                        System.out.println("Aggregated Total Amount funded by Lending Club in IL State : $" + new BigDecimal(totalFundedAmountForStateAgg).toPlainString());
                    }
                }
            }catch(Exception ex){
                System.out.println("Error while processing the aggregated data");
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
