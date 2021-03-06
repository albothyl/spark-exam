package com.coupang.coopers.spark.basic.action.loop;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 전체 데이터를 하나씩 함수로 전달하여 실행하고, 값을 리턴하지 않는다.
 * partition단위로 적용된다. 주의할점은 드라이버 프로그램이 동작하고 있는 서버가 아닌 클러스터의 각 노드(서버)에서 실행된다.
 *
 * @Input : [1, 2, 3, 4, 5]
 * partition 1
 * @Output1 : 1
 * @Output2 : 2
 *
 * partition 2
 * @Output3 : 3
 * @Output4 : 4
 * @Output5 : 5
 */
@Slf4j
public class ForeachPartition implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		ForeachPartition foreachPartition = new ForeachPartition();
		foreachPartition.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5);
		log.warn("INPUT  : " + input);

		JavaRDD<Integer> rdd = sc.parallelize(input, 2);
		rdd.foreachPartition(it -> {
			log.error("Partition");
			it.forEachRemaining(v -> log.warn("Value : " + v));
		});
	}
}
