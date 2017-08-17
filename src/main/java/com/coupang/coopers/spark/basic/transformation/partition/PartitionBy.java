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
 * key를 특정 partition에 할당한다.
 *
 * @Input : [(apple,1), (mouse,1), (monitor,1)]
 * @Output : (monitor,1)
 * @Output : (mouse,1)
 * @Output : (apple,1)
 */
@Slf4j
public class PartitionBy implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		PartitionBy partitionBy = new PartitionBy();
		partitionBy.run();
	}

	private void run() {
		List<Tuple2<String, Integer>> input = Lists.newArrayList(new Tuple2("apple", 1), new Tuple2("mouse", 1), new Tuple2("monitor", 1));
		JavaPairRDD<String, Integer> output;

		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input, 5);
		output = rdd.partitionBy(new HashPartitioner(3));

		log.warn("INPUT  : " + rdd.getNumPartitions());
		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output.getNumPartitions());

		output.foreachPartition((VoidFunction<Iterator<Tuple2<String, Integer>>>) tuple2Iterator -> {
			log.error("=======================");
			while (tuple2Iterator.hasNext()) {
				log.warn("" + tuple2Iterator.next());
			}
		});
	}
}
