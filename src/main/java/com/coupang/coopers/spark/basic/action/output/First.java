package com.coupang.coopers.spark.basic.action.output;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD의 첫번째 값을 출력한다.
 *
 * @Input : [1, 2, 3, 4, 5]
 * @Output : 1
 */
@Slf4j
public class First implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		First first = new First();
		first.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5);
		Integer output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		output = rdd.first();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
