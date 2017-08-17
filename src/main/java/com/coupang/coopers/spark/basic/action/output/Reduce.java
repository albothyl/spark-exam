package com.coupang.coopers.spark.basic.action.output;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD에서 모든 데이터에 대해서 지정한 연산을 수행하여 출력한다.
 *
 * @Input : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
 * @Output : 55
 */
@Slf4j
public class Reduce implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Reduce reduce = new Reduce();
		reduce.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		Integer output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		output = rdd.reduce((v1, v2) -> v1 + v2);

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
