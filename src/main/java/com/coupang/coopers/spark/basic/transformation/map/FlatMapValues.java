package com.coupang.coopers.spark.basic.transformation.map;

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
 * Tuple의 value를 하나의 계층으로 풀고, map을 적용하여 value를 변경한다.
 *
 * @Input : [(1,a,b), (2,a,c), (1,d,e)]
 * @Output : [(1,a), (1,b), (2,a), (2,c), (1,d), (1,e)]
 */
@Slf4j
public class FlatMapValues implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		FlatMapValues flatMapValues = new FlatMapValues();
		flatMapValues.run();
	}

	private void run() {
		List<Tuple2<Integer, String>> input = Lists.newArrayList(new Tuple2(1, "a,b"), new Tuple2(2, "a,c"), new Tuple2(1, "d,e"));
		List<Tuple2<Integer, String>> output;

		JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(input);
		JavaPairRDD<Integer, String> result = rdd
			.flatMapValues(s -> Lists.newArrayList(s.split(",")));

		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
