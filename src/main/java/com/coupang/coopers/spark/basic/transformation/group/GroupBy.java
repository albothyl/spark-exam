package com.coupang.coopers.spark.basic.transformation.group;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * 특정 기준으로 RDD를 기준별로 나눈다.
 *
 * @Input : [1, 2, 3, 4, 5, 6, 7]
 * @Output : [(even,[2, 4, 6]), (odd,[1, 3, 5, 7])]
 */
@Slf4j
public class GroupBy implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		GroupBy groupBy = new GroupBy();
		groupBy.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
		List<Tuple2<String, Iterable<Integer>>> output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		JavaPairRDD<String, Iterable<Integer>> result = rdd.groupBy(i -> i % 2 == 0 ? "even" : "odd");
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
