package com.coupang.coopers.spark.basic.transformation.group;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Tuple (key-value)
 * 여러개의 RDD를 같은 key를 가진 Tuple 묶어 새로운 RDD를 생성한다.
 *
 * @Input : [(a,1), (b,1), (c,1), (b,1), (c,1)]
 * @Output : [(a,[1]), (b,[1, 1]), (c,[1, 1])]
 */
@Slf4j
public class GroupByKey implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		GroupByKey groupByKey = new GroupByKey();
		groupByKey.run();
	}

	private void run() {
		List<Tuple2<String, Integer>> input = Lists.newArrayList(new Tuple2("a",1), new Tuple2("b",1), new Tuple2("c",1), new Tuple2("b",1), new Tuple2("c",1));
		List<Tuple2<String, Iterable<Integer>>> output;

		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
		JavaPairRDD<String, Iterable<Integer>> result = rdd.groupByKey();
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
