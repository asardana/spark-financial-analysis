package com.spark.financial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Aman on 10/14/2017.
 */
public class SparkFinancialAnalysisMain {

    public static void main(String args[]) {

        // Set Spark Configuration
        SparkConf sparkConf = new SparkConf().setAppName("spark-financial-analysis").setMaster("local[*]");
        sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

        // Create Spark Session
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        SparkFinancialAnalysisMain sparkFinMain = new SparkFinancialAnalysisMain();

        // Read the file using SparkSession
        sparkFinMain.readDataFromFile(session);

        // Aggregate data by creating Dataset from JavaBean RDD
        sparkFinMain.aggregateDataUsingReflection(session);

        // Aggregate data by creating Dataset from Row RDD
        sparkFinMain.aggregateDataUsingStructType(session);

    }

    /**
     * Reads the csv file using SparkSession and then prints the schema and data using
     * SparkSession.
     *
     * @param session
     */
    public void readDataFromFile(SparkSession session) {

        Dataset<Row> loanStatsDataset = session.read().csv("/bigdata/LoanStats_2017Q2.csv");
        System.out.println("Printing DataFrame in readDataFromFile");
        loanStatsDataset.printSchema();
        loanStatsDataset.show();
    }

    /**
     * Reads the csv file and coverts it to an RDD of Java beans using reflection. This approach assumes that the schema of the data file
     * is known beforehand and hence RDD of Java beans can be created which can then be converted to Dataset.
     *
     * @param session
     */
    public void aggregateDataUsingReflection(SparkSession session) {

        // Read the Loan Stats in a JavaRDD of Strings
         JavaRDD<String> loanStatsTextRDD = session.read().textFile("/bigdata/LoanStats_2017Q2.csv").javaRDD();

        // Convert the JavaRDD of Strings to JavaRDD of Java Beans
        JavaRDD<LoanDataRecord> loanDataRecordRDD = loanStatsTextRDD.map((line) -> {

            LoanDataRecord loanDataRecord = new LoanDataRecord();

            if (!(line.isEmpty() || line.contains("member_id") || line.contains("Total amount funded in policy code"))) {

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
            } else {
                System.out.println("Invalid Record line " + line);
            }
            return loanDataRecord;
        }).filter(record -> record.getFundedAmt() != null);

        // Print any invalid record
        loanDataRecordRDD.foreach(record -> {
            if(!(record.getPolicyCode().equalsIgnoreCase("1") || record.getPolicyCode().equalsIgnoreCase("2")) )
            if(!(record.getPolicyCode().equalsIgnoreCase("1") || record.getPolicyCode().equalsIgnoreCase("2")) )
            System.out.println("Record iss " + record.toString());
        });

        // Create the Dataset
        Dataset<Row> loanStatFullDataset = session.createDataFrame(loanDataRecordRDD, LoanDataRecord.class);

        loanStatFullDataset.printSchema();

        loanStatFullDataset.show();

        System.out.println("Total records in full data set " + loanStatFullDataset.count());

        // Create the temporary table for querying
        loanStatFullDataset.createOrReplaceTempView("loan_statistics_reflection");

        // Construct the query to filter the records for a given State
        Dataset<Row> loanStatStateFilter = session.sql("SELECT * FROM loan_statistics_reflection where addressState='IL'");

        // Write the filtered record to the file system in CSV format
        loanStatStateFilter.write().mode(SaveMode.Overwrite).csv("/bigdata/loanStatILState.csv");

        loanStatStateFilter.show();

        // Calculate the total amount for a given State
        List<String> fundedAmountsForState = loanStatStateFilter.javaRDD().map(row -> row.getString(2)).collect();

        String total = fundedAmountsForState.stream().reduce((x, y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

        System.out.println("Total Amount funded by Lending Club in IL State : $" + new BigDecimal(total).toPlainString());

        // Calculate the total funding amount for Policy Code 1
        List<String> fundedAmountPolicyCd1 = loanStatFullDataset.javaRDD().filter(row -> "1".equalsIgnoreCase(row.getString(7))).map(row -> row.getString(2)).collect();

        String totalFundedAmountPolicyCd1 = fundedAmountPolicyCd1.stream().reduce((x,y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

        System.out.println("Total Amount funded by Lending Club for Policy Code 1 : $" + new BigDecimal(totalFundedAmountPolicyCd1).toPlainString());
    }

    /**
     * Reads the csv file and coverts it to an RDD of Java beans using StructType Schema. Converts the JavaRDD of Rows to a Dataset
     * which can then be queried to create aggregations
     *
     * @param session
     */
    public void aggregateDataUsingStructType(SparkSession session) {

        // Read the Loan Stats in a JavaRDD of Strings
        JavaRDD<String> loanStatsTextRDD = session.read().textFile("/bigdata/LoanStats_2017Q2.csv").javaRDD();

        // Convert the JavaRDD of Strings to JavaRDD of Row. Also, remove any rows with invalid data or null records
        JavaRDD<Row> loanStatsRowRDD = loanStatsTextRDD.map((line) -> {

            List columnValues = new ArrayList<String>();


            if (!(line.isEmpty() || line.contains("member_id") || line.contains("Total amount funded in policy code"))) {

                // Few records have emp_title with comma separated values resulting in records getting rejected. Cleaning the data before creating Dataset
                String updatedLine = line.replace(", ", "|").replaceAll("[a-z],", "");

                String loanRecordSplits[] = updatedLine.split(",\"");

                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[2]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[3]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[5]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[6]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[8]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[12]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[13]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[23]));
                columnValues.add(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[51]));
            } else {
                System.out.println("Invalid Record line " + line);
            }
            return RowFactory.create(columnValues.toArray());
        }).filter(row -> row.length() != 0);

        // Define the schema for the records. This could be confifurable than being hard coded as String
        String loanDataRecordSchema = "loanAmt, fundedAmt, term, intRate, grade, homeOwnership, annualIncome, addressState, policyCode";

        List<StructField> fields = new ArrayList<StructField>();

        Arrays.stream(loanDataRecordSchema.split(",")).forEach(fieldName -> fields.add(DataTypes.createStructField(fieldName.trim(), DataTypes.StringType, true)));

        // Create the StructType schema using the StructFields
        StructType schema = DataTypes.createStructType(fields);

        System.out.println("LoanDataRecord Schema " + schema.toString());

        Dataset<Row> loanStatFullDataset = session.createDataFrame(loanStatsRowRDD, schema);

        loanStatFullDataset.printSchema();

        loanStatFullDataset.show();

        // Create the temporary table for querying
        loanStatFullDataset.createOrReplaceTempView("loan_statistics_structtype");

        // Construct the query to filter the records for a given State
        Dataset<Row> loanStatStateFilter = session.sql("SELECT * FROM loan_statistics_structtype where addressState='IL'");

        loanStatStateFilter.show();

        loanStatStateFilter.write().mode(SaveMode.Overwrite).parquet("/bigdata/loanStatStateFilter.parquet");

        // This is to demonstarte how the data stored in the parquet can be read into a Dataset

        Dataset<Row> loanStatStateParquet = session.read().parquet("/bigdata/loanStatStateFilter.parquet");

        System.out.println("Printing DataFrame after reading from Parquet");

        loanStatStateParquet.show();

        // Calculate the total amount for a given State
        List<String> fundedAmountsForState = loanStatStateParquet.javaRDD().map(row -> row.getString(1)).collect();

        String total = fundedAmountsForState.stream().reduce((x, y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

        System.out.println("Total Amount funded by Lending Club in IL State : $" + new BigDecimal(total));


    }


}
