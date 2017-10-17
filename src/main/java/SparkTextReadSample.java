/**
 * Created by aryad on 14/10/2017.
 * For more info this link is useful: <a href="http://spark.apache.org/docs/2.1.0/programming-guide.html">spark-programming-guide</a>
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

        sc.stop();

        System.out.println("Total Length: " + totalLength);
    }

}

