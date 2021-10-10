import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkSqlMain {
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "c:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    final SparkSession sparkSession =
        SparkSession.builder()
            .appName("spark-sql-example")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
            .getOrCreate();

    final Dataset<Row> students =
        sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

    Dataset<Row> mathResults =
        students.filter(
            functions.col("subject").equalTo("Math").and(functions.col("year").geq(2007)));

    mathResults.show();

    sparkSession.close();
  }
}
