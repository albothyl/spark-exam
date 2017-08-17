package com.coupang.coopers.spark.basic.transformation.aggregation;

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
 * 두 RDD의 모든 경우의 수를 구한다. (카타시안곱)
 *
 * @Input1 : [1, 2, 3]
 * @Input2 : [a, b, c]
 * @Output : [(1,a), (1,b), (1,c), (2,a), (2,b), (2,c), (3,a), (3,b), (3,c)]
 */
@Slf4j
public class Cartesian implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Cartesian cartesian = new Cartesian();
		cartesian.run();
	}

	private void run() {
		List<Integer> input_1 = Lists.newArrayList(1, 2, 3);
		List<String> input_2 = Lists.newArrayList("a", "b", "c");
		List<Tuple2<Integer, String>> output;

		JavaRDD<Integer> rdd_1 = sc.parallelize(input_1);
		JavaRDD<String> rdd_2 = sc.parallelize(input_2);
		JavaPairRDD<Integer, String> result = rdd_1.cartesian(rdd_2);
		output = result.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT : " + output);
	}
}
