package com.coupang.coopers.spark.basic.transformation.filter;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 특정 조건에 해당하는 값들만 추출한다.
 *
 * @Input : [1, 2, 3, 4, 5]
 * @Output : [3, 4, 5]
 */
@Slf4j
public class Filter implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Filter filter = new Filter();
		filter.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5);
		List<Integer> output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		JavaRDD<Integer> result = rdd.filter(i -> i > 2);
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
