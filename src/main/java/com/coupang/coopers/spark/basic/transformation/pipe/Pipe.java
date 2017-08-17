package com.coupang.coopers.spark.basic.transformation.pipe;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD에 리눅스 명령어를 적용한다.
 *
 * @Input : [1, 2, 3, 4, 5, 6, 7, 8, 9]
 * @Output : [1, 3, 4, 6, 7, 9]
 */
@Slf4j
public class Pipe implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Pipe pipe = new Pipe();
		pipe.run();
	}

	private void run() {
		List<String> input = Lists.newArrayList("1, 2, 3", "4, 5, 6", "7, 8, 9");
		List<String> output;

		JavaRDD<String> rdd = sc.parallelize(input);
		JavaRDD<String> result = rdd.pipe("cut -f 1,3 -d ,"); //linux의 cut 유틸리티를 이용해 분리한 뒤 1번째와 3번째 값을 추출
		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}
}
