package com.coupang.coopers.spark.basic.action.output;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD에서 모든 데이터에 대해서 지정한 연산을 수행하여 출력하고, 초기값을 지정할 수 있다.
 * 초기값은 partition별 부분 병합을 수행할 때마다 사용되기 때문에 여러번 적용돼도 문제가 없는 값을 사용해야 한다.
 * ex: 0(초기값) + 1 = 1  or  1(초기값) * 2 = 2
 *
 * @Input : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
 * @Output : 55
 */
@Slf4j
public class Fold implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Fold fold = new Fold();
		fold.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		Integer output;
		Integer initValue = 0;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		output = rdd.fold(initValue, (v1, v2) -> v1 + v2);

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
