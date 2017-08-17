package com.coupang.coopers.spark.basic.transformation.aggregation;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 중복된 데이터를 제거한다.
 *
 * @Input : [1, 2, 3, 1, 2, 3, 1, 2, 3]
 * @Output : [1, 2, 3]
 */
@Slf4j
public class Distinct implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Distinct distinct = new Distinct();
		distinct.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 1, 2, 3, 1, 2, 3);
		List<Integer> output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		JavaRDD<Integer> result = rdd.distinct();
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
