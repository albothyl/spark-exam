package com.coupang.coopers.spark.basic.action.output;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD에서 모든 데이터를 더하여 출력한다. Double type만 사용할 수 있다.
 *
 * @Input : [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
 * @Output : 55.0
 */
@Slf4j
public class Sum implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Sum sum = new Sum();
		sum.run();
	}

	private void run() {
		List<Double> input = Lists.newArrayList(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d);
		Double output;

		JavaDoubleRDD rdd = sc.parallelizeDoubles(input);
		output = rdd.sum();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
