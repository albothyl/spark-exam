package com.coupang.coopers.spark.basic.transformation.partition;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * partition의 갯수를 지정한 갯수로 설정한다.
 * repartition은 partition의 갯수를 늘리거나 줄일 수 있는데 coalesce가 존재하는 이유는 셔플로 발생하는 성능차이 때문이다.
 * repartition은 서플을 사용하기 때문에 partition을 늘릴때 주로 사용한다.
 *
 * @Input : 10 (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
 * @Output1 : 15
 * @Output2 : 5
 */
@Slf4j
public class Repartition implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Repartition repartition = new Repartition();
		repartition.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> output_1;
		JavaRDD<Integer> output_2;

		JavaRDD<Integer> rdd = sc.parallelize(input, 10);
		output_1 = rdd.repartition(15);
		output_2 = rdd.repartition(5);

		log.warn("INPUT  : " + rdd.getNumPartitions());
		log.warn("OUTPUT 1 : " + output_1.getNumPartitions());
		log.warn("OUTPUT 2 : " + output_2.getNumPartitions());
	}
}
