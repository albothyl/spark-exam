package com.coupang.coopers.spark.basic.action.other;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD의 partition갯수, 의존성 정보 등 debug 정보를 출력한다.
 *
 * @Input : [1, 2, 3, 4, 5]
 * @Output : (1) MapPartitionsRDD[1] at map at ToDebugString.java:34 []
 */
@Slf4j
public class ToDebugString implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		ToDebugString toDebugString = new ToDebugString();
		toDebugString.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5);
		String output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		JavaRDD<Integer> result = rdd.map(i -> i + 1);
		output = result.toDebugString();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
