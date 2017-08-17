package com.coupang.coopers.spark.basic.transformation.partition;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * partition의 갯수를 지정한 만큼 줄인다. (partition을 조정하려는 RDD의 partition갯수보다 클 수 없다.)
 * repartition은 partition의 갯수를 늘리거나 줄일 수 있는데 coalesce가 존재하는 이유는 셔플로 발생하는 성능차이 때문이다.
 * coalesce는 강제로 셔플을 수행하는 옵션을 지정하지 않는한 셔플을 사용하지 않기 때문에 성능이 더 좋다.
 * 데이터 필터링 등의 작업으로 데이터 수가 줄어들어 파티션의 수를 줄이고자 할 때 주로 사용한다.
 *
 * @Input : 10 (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
 * @Output : 5
 */
@Slf4j
public class Coalesce implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Coalesce coalesce = new Coalesce();
		coalesce.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> output;

		JavaRDD<Integer> rdd = sc.parallelize(input, 10);
		output = rdd.coalesce(5);

		log.warn("INPUT  : " + rdd.getNumPartitions());
		log.warn("OUTPUT : " + output.getNumPartitions());
	}
}
