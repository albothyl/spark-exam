package com.coupang.coopers.spark.basic.transformation.sort;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Tuple (key-value)
 * key를 기준으로 오름차순 정렬한다.
 *
 * @Input : [(q,1), (z,1), (a,1)]
 * @Output : [(a,1), (q,1), (z,1)]
 */
@Slf4j
public class SortByKey implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		SortByKey sortByKey = new SortByKey();
		sortByKey.run();
	}

	private void run() {
		List<Tuple2<String, Integer>> input = Lists.newArrayList(new Tuple2("q", 1), new Tuple2("z", 1), new Tuple2("a", 1));
		List<Tuple2<String, Integer>> output;

		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
		JavaPairRDD<String, Integer> result = rdd.sortByKey();
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
