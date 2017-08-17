package com.coupang.coopers.spark.basic.transformation.map;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * MapPartitions와 동일하나 index정보를 추가적으로 받아서 처리한다.
 *
 * @Input : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
 * @Output : [4, 5, 6]
 */
@Slf4j
public class MapPartitionsWithIndex implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		MapPartitionsWithIndex mapPartitionsWithIndex = new MapPartitionsWithIndex();
		mapPartitionsWithIndex.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		List<Integer> output;

		JavaRDD<Integer> rdd = sc.parallelize(input, 3);
		JavaRDD<Integer> result = rdd.mapPartitionsWithIndex((idx, i) -> {
			List<Integer> partitionCheckList = Lists.newArrayList(i);
			log.warn("idx : {} | i : {}", idx, partitionCheckList);

			List<Integer> partition = Lists.newArrayList();

			if (idx == 1) {
				partition.addAll(partitionCheckList);
			}

			return partition.iterator();
		}, true);
		output = result.collect();

		log.warn("INPUT : " + input);
		log.warn("OUTPUT : " + output);
	}
}
