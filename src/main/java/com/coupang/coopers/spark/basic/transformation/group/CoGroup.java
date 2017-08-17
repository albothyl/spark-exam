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
 * 여러개의 RDD를 같은 key의 시퀀스로 구성된 Tuple로 묶어준다.
 *
 * @Input 1 : [(k1,v1), (k2,v2), (k1,v3)]
 * @Input 2 : [(k1,v4)]
 * @Output : [(k2,([v2],[])), (k1,([v1, v3],[v4]))]
 */
@Slf4j
public class CoGroup implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		CoGroup coGroup = new CoGroup();
		coGroup.run();
	}

	private void run() {
		List<Tuple2<String, String>> input_1 = Lists.newArrayList(new Tuple2("k1", "v1"), new Tuple2("k2", "v2"), new Tuple2("k1", "v3"));
		List<Tuple2<String, String>> input_2 = Lists.newArrayList(new Tuple2("k1", "v4"));
		List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> output;

		JavaPairRDD<String, String> rdd_1 = sc.parallelizePairs(input_1);
		JavaPairRDD<String, String> rdd_2 = sc.parallelizePairs(input_2);
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> result = rdd_1.cogroup(rdd_2);
		output = result.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT : " + output);
	}
}
