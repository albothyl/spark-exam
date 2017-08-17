package com.coupang.coopers.spark.basic.transformation.other;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD에서 sample을 추출한다.
 * 첫번째 Input값인 withReplacement가 true/false에 따라 sample로 추출되는 값이 틀리다.
 * true (복원 추출) : sample내에서 각 요소가 나타내는 횟수에 대한 기대값, 즉 각 요소의 평균 발생 횟수를 의미하고 반드시 0 이상의 값을 지정해야 한다.
 * false (비복원 추출) : 각 요소가 sample에 포함될 혹률을 의미하며, 0 ~ 1 사이의 값을 지정해야 한다
 *
 * @Input : [1, 2, 3, 4, 5]
 * @Output1 : [1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 5]
 * @Output2 : [1, 4]
 */
@Slf4j
public class Sample implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Sample sample = new Sample();
		sample.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5);
		List<Integer> output_1;
		List<Integer> output_2;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		JavaRDD<Integer> result_1 = rdd.sample(true, 1.5);
		JavaRDD<Integer> result_2 = rdd.sample(false, 0.5);
		output_1 = result_1.collect();
		output_2 = result_2.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT 1 : " + output_1);
		log.warn("OUTPUT 2 : " + output_2);
	}
}
