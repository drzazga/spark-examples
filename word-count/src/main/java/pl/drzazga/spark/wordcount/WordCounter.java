package pl.drzazga.spark.wordcount;

import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class WordCounter {
    
    private static final Logger log = LoggerFactory.getLogger(WordCounter.class);

    private static final String MASTER = "local[*]";
    
	public static void main(String[] args) {
	    new WordCounter().calculate("/bible.txt");
	}
	
    private void calculate(String resourceFileName) {
        SparkConf conf = new SparkConf().setAppName(WordCounter.class.getName()).setMaster(MASTER);

        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            context.textFile(getClass().getResource(resourceFileName).toURI().toString())
                   .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
                   .filter(s -> s.length() > 0)
                   .mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1))
                   .reduceByKey(Integer::sum)
                   .mapToPair(x -> x.swap())
                   .sortByKey(false, 1)
                   .mapToPair(x -> x.swap())
                   .take(10)
                   .forEach(result -> log.info(String.format("'%s' => %d", result._1(), result._2)));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
