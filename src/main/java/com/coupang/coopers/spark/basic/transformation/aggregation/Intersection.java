package com.coupang.coopers.spark.basic.transformation.aggregation;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 두 RDD의 공통값으로 RDD를 만든다. (교집합)
 *
 * @Input1 : [a, a, b, c]
 * @Input2 : [a, a, c, c]
 * @Output : [a, c]
 */
@Slf4j
public class Intersection implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Intersection intersection = new Intersection();
		intersection.run();
	}

	private void run() {
		List<String> input_1 = Lists.newArrayList("a", "a", "b", "c");
		List<String> input_2 = Lists.newArrayList("a", "a", "c", "c");
		List<String> output;

		JavaRDD<String> rdd_1 = sc.parallelize(input_1);
		JavaRDD<String> rdd_2 = sc.parallelize(input_2);
		JavaRDD<String> result = rdd_1.intersection(rdd_2);
		output = result.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT : " + output);
	}
}
