package com.coupang.coopers.spark.basic.transformation.aggregation;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 두 RDD의 값을 합친다. (합집합)
 *
 * @Input1 : [a, b, c]
 * @Input2 : [d, e, f]
 * @Output : [a, b, c, d, e, f]
 */
@Slf4j
public class Union implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Union union = new Union();
		union.run();
	}

	private void run() {
		List<String> input_1 = Lists.newArrayList("a", "b", "c");
		List<String> input_2 = Lists.newArrayList("d", "e", "f");
		List<String> output;

		JavaRDD<String> rdd_1 = sc.parallelize(input_1);
		JavaRDD<String> rdd_2 = sc.parallelize(input_2);
		JavaRDD<String> result = rdd_1.union(rdd_2);
		output = result.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT : " + output);
	}
}
