package com.coupang.coopers.spark.basic.transformation.map;

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
 * (Tuple) key-value로 이루어진 Tuple의 value에 map을 적용하여 value를 변경한다.
 *
 * @Input : [a, b, c]
 * @Output : [(a,2), (b,2), (c,2)]
 */
@Slf4j
public class MapValues implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		MapValues mapValues = new MapValues();
		mapValues.run();
	}

	private void run() {
		List<String> input = Lists.newArrayList("a", "b", "c");
		List<Tuple2<String, Integer>> output;

		JavaRDD<String> rdd = sc.parallelize(input);
		JavaPairRDD<String, Integer> result = rdd
			.mapToPair(i -> new Tuple2<>(i, 1))
			.mapValues(n -> n + 1);

		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
