/**
 * Created by aryad on 15/10/2017.
 * For this sample to run a Postgresql Database is needed to be installed.
 * Username, password, database and table name for database are in user, password, database and table variables.
 * In order to fill the database in sql file in resources should be run.
 * For more info this link is useful: <a href="https://spark.apache.org/docs/latest/sql-programming-guide.html">spark-sql-programming-guide</a>
 */

import java.util.Properties;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;

public class SparkSQLdataReadSample {

    private final static String user = "postgres";
    private final static String password = "postgres";
    private final static String database = "postgres";
    private final static String table = "people";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();

        runJdbcDatasetExample(spark);

        spark.stop();
    }

    private static void runJdbcDatasetExample(SparkSession spark) {
        // $example on:jdbc_dataset$
        // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        // Loading data from a JDBC source
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:" + database)
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .load();

        jdbcDF.show();
        jdbcDF.select("name").show();

        // Map rows to ages
        Dataset<Integer> agesDS = jdbcDF.map((MapFunction<Row, Integer>) row -> row.getInt(1),
                Encoders.INT());

        // Reduce to calculate sum of all ages
        Integer sumAges = agesDS.reduce((ReduceFunction<Integer>) (a, b) -> a + b);

        System.out.println("Sum Ages: " + sumAges);


        // Loading data using jdbc methods
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:postgresql:" + database, table, connectionProperties);


        // Saving data to a JDBC source
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql:" + database)
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .mode(SaveMode.Overwrite)
                .save();

        // Saving data using jdbc methods
        jdbcDF2.write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:postgresql:" + database, table, connectionProperties);

        // Specifying create table column data types on write
        jdbcDF.write()
                .mode(SaveMode.Overwrite)
                .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
                .jdbc("jdbc:postgresql:" + database, table, connectionProperties);
        // $example off:jdbc_dataset$
    }
}
