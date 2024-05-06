package ca.antany.spark.learning;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.expressions.Window;

public class GroupByFunctions {

	public static void main(String[] args) {
		var spark = Common.getSparkSession();
		var df = spark.read().option("header", true).csv("file:///data/retail-data/all");
		
		df.createOrReplaceTempView("test_table");
		
		df.show();
		
		//var fun = Window.partitionBy(null)
		
		spark.sql("SELECT COUNTRY,STOCKcODE, count(1) FROM TEST_TABLE where UPPER(country) LIKE 'UNITED%' and stockcode in ('22139') GROUP BY COUNTRY,STOCKcODE GROUPING SETS ((STOCKcODE),(COUNTRY))").show();
		
		spark.sql("SELECT COUNTRY,STOCKcODE, count(1) FROM TEST_TABLE where UPPER(country) LIKE 'UNITED%' and stockcode in ('22139') GROUP BY COUNTRY,STOCKcODE with rollup ").show();
		
		
		spark.sql("SELECT COUNTRY,STOCKcODE, count(1) FROM TEST_TABLE where UPPER(country) LIKE 'UNITED%' and stockcode in ('22139') GROUP BY COUNTRY,STOCKcODE with cube ").show();
		
		
		//df.groupBy(col("Country")).count().show();
	}
}
