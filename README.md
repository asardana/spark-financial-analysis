# spark-financial-analysis
Sample application demonstrating how to use Spark for analyzing financial data made freely available by Lending Club. For more details, please refer the following blog post.

https://asardana.com/2017/10/15/analyzing-financial-data-with-apache-spark-2-2/

https://asardana.com/2017/10/21/introduction-to-stream-processing-using-apache-spark/


With the rise of big data processing in the Enterprise world, it’s quite evident that Apache Spark has become one of the most popular framework to process large amount of data to both in the batch mode and real-time. This article won’t go into the overview of Apache Spark since there is already many good references available to get the basic understanding of Apache Spark and how it provides fast in-memory processing at scale by the way abstracting the underlying data in the form of Resilient Distributed Dataset (RDD).

We’ll loot at some of the examples to see how Apache Spark can be used to ingest the financial data and then process it at large-scale to do some computations and aggregations. We’ll use Apache Spark 2.2 which provides further abstraction to RDD in the form of Dataset. Dataset are analogous to distributed relational database and can be thought of  RDDs with schema applied to the data. This provides the capability to query the data just like the relational database on top of Apache Spark framework. The APIs for providing the SQL like construct on top of RDDs is available via the Spark SQL library.

Let’s look at a sample Java application demonstrating how Spark SQL can be used to process large amounts of distributed data at scale.

The financial data used in this application is provided by Lending Club. The data has records for all the loans issued and includes the loan amount, funding amount, term, interest rate etc. along with lot of other details pertaining to the loan issued to the customer.

The code discussed in this post is available on Github.

In order to get started, create a new Java project and include the following dependencies.

compile group: 'org.apache.spark', name: 'spark-core_2.10', version: '2.2.0'
compile group: 'org.apache.spark', name: 'spark-streaming_2.10', version: '2.2.0'
compile group: 'org.apache.spark', name: 'spark-sql_2.10', version: '2.2.0'

SparkFinancialAnalysisMain is the main class where we define the Spark Configuration. In this case we are running the Spark application locally and hence the master is set as “local[*]” to use as many logical cores available in the local machine. This helps in running the tasks in parallel on the distributed data available in the memory for fast processing.

1
2
3
// Set Spark Configuration
SparkConf sparkConf = new SparkConf().setAppName("spark-financial-analysis").setMaster("local[*]");
sparkConf.set("spark.sql.parquet.compression.codec", "snappy");
 

After setting the configuration, we need to create a Spark Session that provides an entry point to the functionality in Spark.

1
2
// Create Spark Session
SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();
There are 3 methods showing how to read the financial data provided by the Lending Club and running some aggregations to get some meaningful insights from the raw data.

1
2
3
4
5
6
7
8
// Read the file using SparkSession
sparkFinMain.readDataFromFile(session);
 
// Aggregate data by creating Dataset from JavaBean RDD
sparkFinMain.aggregateDataUsingReflection(session);
 
// Aggregate data by creating Dataset from Row RDD
sparkFinMain.aggregateDataUsingStructType(session);
 

Method readDataFromFile reads the CSV file “LoanStats_2017Q2.csv” stored in a temp directory and converts it to a Dataset of Rows. We can then print the schema and some sample records showing the data in a tabular format. It’s interesting to note that Spark assigns a schema to the data with default being String columns which could be null.

Spark1

Spark2

Method aggregateDataUsingReflection reads the data as a text file and converts it into an RDD of Strings.

1
2
// Read the Loan Stats in a JavaRDD of Strings
 JavaRDD&lt;String&gt; loanStatsTextRDD = session.read().textFile("/bigdata/LoanStats_2017Q2.csv").javaRDD();
 

RDD of Strings is then converted to an RDD of Java Beans. The String RDD is mapped to an RDD of LoanDataRecord beans. RDD of beans is then converted to a DataFrame which is basically a Dataset of Rows. Dataset of Rows represents an RDD with some schema applied to the underlying data. This approach is useful when the schema of the data can be represented in a form of Java Bean.

1
2
// Create the Dataset
Dataset&lt;Row&gt; loanStatFullDataset = session.createDataFrame(loanDataRecordRDD, LoanDataRecord.class);
 

Spark3

Finally we create a temporary table from the Dataset that can be queried using the standard SQL constructs.

1
2
3
4
5
// Create the temporary table for querying
loanStatFullDataset.createOrReplaceTempView("loan_statistics_reflection");
 
// Construct the query to filter the records for a given State
Dataset<Row> loanStatStateFilter = session.sql("SELECT * FROM loan_statistics_reflection where addressState='IL'");
Spark4.jpg

The result of the SQL query is another Dataset which can be saved on the file system as a CSV file.

1
2
// Write the filtered record to the file system in CSV format
loanStatStateFilter.write().mode(SaveMode.Overwrite).csv("/bigdata/loanStatILState.csv");
The Dataset can be converted back to an RDD and we can run various transformations and actions to compute the aggregations. In this example, we are calculating the total amount funded by Lending Club in the state of Illinois. Another example is to calculate the total amount funded for Policy Code 1.

1
2
3
4
5
6
// Calculate the total amount for a given State
List<String> fundedAmountsForState = loanStatStateFilter.javaRDD().map(row -> row.getString(2)).collect();
 
String total = fundedAmountsForState.stream().reduce((x, y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();
 
System.out.println("Total Amount funded by Lending Club in IL State : $" + new BigDecimal(total).toPlainString());
Spark5

Method aggregateDataUsingStructType  is similar to aggregateDataUsingReflection   in functionality, except the way in which the DataFrame is created from the JavaRDD of strings. This approach is useful in the scenarios where the schema cannot be represented as Java Beans or it’s not known beforehand.

JavaRDD of strings is converted to an RDD of Row objects. This is achieved by using the RowFactory that takes the column values as input.

1
return RowFactory.create(columnValues.toArray());
Then the schema is created using a StructType and finally applied to the Row RDD to create the DataFrame.

1
2
3
4
5
6
7
8
9
10
11
12
13
// Define the schema for the records. This could be confifurable than being hard coded as String
String loanDataRecordSchema = "loanAmt, fundedAmt, term, intRate, grade, homeOwnership, annualIncome, addressState, policyCode";
 
List<StructField> fields = new ArrayList<StructField>();
 
Arrays.stream(loanDataRecordSchema.split(",")).forEach(fieldName -> fields.add(DataTypes.createStructField(fieldName.trim(), DataTypes.StringType, true)));
 
// Create the StructType schema using the StructFields
StructType schema = DataTypes.createStructType(fields);
 
System.out.println("LoanDataRecord Schema " + schema.toString());
 
Dataset<Row> loanStatFullDataset = session.createDataFrame(loanStatsRowRDD, schema);
This example also demonstrates how the data can be saved in a compressed columnar Parquet format. The data can be read back from the Parquet format and converted to an RDD.

1
2
3
4
5
loanStatStateFilter.write().mode(SaveMode.Overwrite).parquet("/bigdata/loanStatStateFilter.parquet");
 
// This is to demonstarte how the data stored in the parquet can be read into a Dataset
 
Dataset<Row> loanStatStateParquet = session.read().parquet("/bigdata/loanStatStateFilter.parquet");
Running this method gives us the same results as we saw in the previous method.

Spark6.jpg

In the next post, we’ll look at how Spark Streaming can be leveraged to compute the aggregations on the stream of data in real time instead of processing the data in batch mode.
