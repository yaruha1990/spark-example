import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Main {
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "c:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    final JavaRDD<String> stringsJavaRDD =
        sparkContext.textFile("src/main/resources/subtitles/input.txt");

    final JavaRDD<String> lettersOnlyRDD =
        stringsJavaRDD
            .map(string -> string.replaceAll("[^a-zA-Z\\s]", ""))
            .filter(string -> string.trim().length() > 0)
            .map(String::toLowerCase);

    final JavaPairRDD<String, Long> wordsAndOnesRDD =
        lettersOnlyRDD
            .flatMap(letter -> Arrays.asList(letter.split(" ")).iterator())
            .filter(Util::isNotBoring)
            .filter(word -> !isBlank(word))
            .mapToPair((PairFunction<String, String, Long>) s -> new Tuple2<>(s, 1L));

    final JavaPairRDD<String, Long> wordAndAmount =
        wordsAndOnesRDD.reduceByKey((Function2<Long, Long, Long>) Long::sum);

    final JavaPairRDD<Long, String> amountAndWord = wordAndAmount.mapToPair(Tuple2::swap);

    final JavaPairRDD<Long, String> sortedAmountAndWord = amountAndWord.sortByKey(false);

    //    final List<String> mostCommonWords = sortedAmountAndWord.map(Tuple2::_2).take(20);

    sortedAmountAndWord.take(20).forEach(System.out::println);

    sparkContext.close();
  }
}
