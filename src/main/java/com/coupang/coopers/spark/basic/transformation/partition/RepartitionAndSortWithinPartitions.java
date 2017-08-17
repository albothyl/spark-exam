package com.coupang.coopers.spark.basic.transformation.partition;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple (key-value)
 * 특정 기준에 따라 여러개의 partition으로 분리하고 각 파티션 단위로 정렬을 수행한 뒤 이 결과로 새로운 RDD를 생성한다.
 *
 * @Input : 10 (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
 * @Output1 : [(0,-), (3,-), (6,-), (9,-)]
 * @Output2 : [(1,-), (4,-), (7,-)]
 * @Output2 : [(2,-), (5,-), (8,-)]
 */
@Slf4j
public class RepartitionAndSortWithinPartitions implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		RepartitionAndSortWithinPartitions repartitionAndSortWithinPartitions = new RepartitionAndSortWithinPartitions();
		repartitionAndSortWithinPartitions.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaPairRDD<Integer, String> output;

		JavaPairRDD<Integer, String> rdd = sc.parallelize(input, 10).mapToPair(i -> new Tuple2(i, "-"));
		output = rdd.repartitionAndSortWithinPartitions(new HashPartitioner(3));

		output.foreachPartition((VoidFunction<Iterator<Tuple2<Integer, String>>>) tuple2Iterator -> {
			log.error("=======================");
			while (tuple2Iterator.hasNext()) {
				log.warn("" + tuple2Iterator.next());
			}
		});
	}
}
