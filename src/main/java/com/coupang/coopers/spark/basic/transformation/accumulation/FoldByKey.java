package com.coupang.coopers.spark.basic.transformation.accumulation;

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
 * 여러개의 RDD를 같은 key를 가진 Tuple의 value를 계산하여 새로운 RDD를 생성하는데 계산할때 초기값을 적용할 수 있다.
 *
 * @Input : [(a,1), (b,1), (b,1)], InitValue : 10
 * @Output : [(a,11), (b,12)]
 */
@Slf4j
public class FoldByKey implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		FoldByKey foldByKey = new FoldByKey();
		foldByKey.run();
	}

	private void run() {
		List<Tuple2<String, Integer>> input = Lists.newArrayList(new Tuple2("a",1), new Tuple2("b",1), new Tuple2("b",1));
		List<Tuple2<String, Integer>> output;
		Integer initValue = 10;

		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
		JavaPairRDD<String, Integer> result = rdd.foldByKey(initValue, (i1, i2) -> i1 + i2);
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
