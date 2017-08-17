package com.coupang.coopers.spark.basic.action.output;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD에서 지정한 갯수만큼 random한 index의 데이터를 출력한다.
 *
 * @Input : [1, 2, 3, 4, 5]
 * @Output : [?, ?, ?]
 */
@Slf4j
public class TakeSample implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		TakeSample takeSample = new TakeSample();
		takeSample.run();
	}

	private void run() {
		List<Integer> input = Lists.newArrayList(1, 2, 3, 4, 5);
		List<Integer> output;

		JavaRDD<Integer> rdd = sc.parallelize(input);
		output = rdd.takeSample(false, 3);

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
