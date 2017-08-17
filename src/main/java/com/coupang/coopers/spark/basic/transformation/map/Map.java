package com.coupang.coopers.spark.basic.transformation.map;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 데이터를 변환한다.
 *
 * @Input : [1, 2, 3, 4, 5]
 * @Output : [2, 3, 4, 5, 6]
 */
@Slf4j
public class Map implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Map map = new Map();
		map.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5);
		List<Integer> output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		JavaRDD<Integer> result = rdd.map(i -> i + 1);
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
