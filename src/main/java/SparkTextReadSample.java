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
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./src/main/resources/sampleEventsLog");
        JavaRDD<Integer> lineLengths = lines.map(String::length);
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println(totalLength);
    }

}

