/**
 * Created by aryad on 14/10/2017.
 */


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class SparkTextReadSample {

    public static void main(String[] args) {

        String appName = "Length Count";
        String master = "local";
        // initialize spark configuration with application name and given master
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Read sample file for counting
        JavaRDD<String> lines = sc.textFile("./src/main/resources/sampleEventsLog");
        // Map lines to the length of lines
        JavaRDD<Integer> lineLengths = lines.map(String::length);
        // Reduce to calculate total sum
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println(totalLength);
    }

}

