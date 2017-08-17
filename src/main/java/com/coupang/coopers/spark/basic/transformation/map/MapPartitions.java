package com.coupang.coopers.spark.basic.transformation.map;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 전체 input을 지정한 갯수의 부분 집합으로 나누어 처리한다.
 *
 * @Input : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
 * @Output : [1, 2, 3] | [4, 5, 6] | [7, 8, 9, 10]
 */
@Slf4j
public class MapPartitions implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		MapPartitions mapPartitions = new MapPartitions();
		mapPartitions.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		List<Integer> output;

		JavaRDD<Integer> rdd = sc.parallelize(input, 3);
		JavaRDD<Integer> result = rdd.mapPartitions(i -> {
			List<Integer> partition = Lists.newArrayList();
			i.forEachRemaining(partition::add);
			log.warn("RESULT : " + partition.toString());

			return partition.iterator();
		});
		output = result.collect();

		log.warn("INPUT : " + input);
		log.warn("OUTPUT : " + output);
	}
}
