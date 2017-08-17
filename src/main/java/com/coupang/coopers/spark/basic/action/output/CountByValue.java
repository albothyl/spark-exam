package com.coupang.coopers.spark.basic.action.output;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * RDD에서 동일한 갯수의 데이터가 몇개인지 출력한다.
 *
 * @Input : [1, 1, 2, 3, 3]
 * @Output : {1=2, 3=2, 2=1}
 */
@Slf4j
public class CountByValue implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		CountByValue countByValue = new CountByValue();
		countByValue.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 1, 2, 3, 3);
		Map<Integer, Long> output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		output = rdd.countByValue();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
