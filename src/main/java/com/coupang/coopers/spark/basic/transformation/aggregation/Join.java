package com.coupang.coopers.spark.basic.transformation.aggregation;

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
 * 두 RDD_1, RDD_2를 같은 key로 묶어 새로운 RDD를 생성한다.
 *
 * @Input1 : [(a,1), (b,1), (c,1), (d,1), (e,1)]
 * @Input2 : [(b,2), (c,2)]
 * @Output : [(b,(1,2)), (c,(1,2))]
 */
@Slf4j
public class Join implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Join join = new Join();
		join.run();
	}

	private void run() {
		List<Tuple2<String, Integer>> input_1 = Lists.newArrayList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("c", 1), new Tuple2("d", 1), new Tuple2("e", 1));
		List<Tuple2<String, Integer>> input_2 = Lists.newArrayList(new Tuple2("b", 2), new Tuple2("c", 2));
		List<Tuple2<String, Tuple2<Integer, Integer>>> output;

		JavaPairRDD<String, Integer> rdd_1 = sc.parallelizePairs(input_1);
		JavaPairRDD<String, Integer> rdd_2 = sc.parallelizePairs(input_2);
		JavaPairRDD<String, Tuple2<Integer, Integer>> result = rdd_1.join(rdd_2);
		output = result.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT : " + output);
	}
}
