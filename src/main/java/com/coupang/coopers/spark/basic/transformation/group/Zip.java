package com.coupang.coopers.spark.basic.transformation.group;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * 서로 다른 RDD를 index를 기준으로 하나의 Tuple (key-value)로 묶어 RDD로 생성한다.
 *
 * @Input1 : [a, b, c]
 * @Input2 : [1, 2, 3]
 * @Output : [(a,1), (b,2), (c,3)]
 */
@Slf4j
public class Zip implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Zip zip = new Zip();
		zip.run();
	}

	private void run() {
		List<String> input_1 = Lists.newArrayList("a", "b", "c");
		List<Integer> input_2 = Lists.newArrayList(1, 2, 3);
		List<Tuple2<String, Integer>> output;

		JavaRDD<String> rdd_1 = sc.parallelize(input_1);
		JavaRDD<Integer> rdd_2 = sc.parallelize(input_2);
		JavaPairRDD<String, Integer> result = rdd_1.zip(rdd_2);
		output = result.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT : " + output);
	}
}
