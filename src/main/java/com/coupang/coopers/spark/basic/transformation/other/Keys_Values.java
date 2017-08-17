package com.coupang.coopers.spark.basic.transformation.other;

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
 * RDD에 있는 key와 valuse를 추출한다.
 *
 * @Input : [(k1,v2), (k2,v2), (k3,v3)]
 * @Output (keys) : [k1, k2, k3]
 * @Output (values) : [v2, v2, v3]
 */
@Slf4j
public class Keys_Values implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Keys_Values keys_values = new Keys_Values();
		keys_values.run();
	}

	private void run() {
		List<Tuple2<String, String>> input = Lists.newArrayList(new Tuple2("k1", "v2"), new Tuple2("k2", "v2"), new Tuple2("k3", "v3"));
		List<String> output_1;
		List<String> output_2;

		JavaPairRDD<String, String> rdd = sc.parallelizePairs(input);
		JavaPairRDD<String, String> result = rdd.sortByKey();
		output_1 = result.keys().collect();
		output_2 = result.values().collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT 1 (keys) : " + output_1);
		log.warn("OUTPUT 2 (values) : " + output_2);
	}
}
