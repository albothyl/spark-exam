package com.coupang.coopers.spark.basic.transformation.map;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 같은 타입 여러 계층의 데이터를 하나의 계층으로 풀어서 처리한다.
 *
 * @Input : ["apple,orange", "grape,apple,mango", "blueberry,tomato,orange"]
 * @Output : ["apple, orange, grape, apple, mango, blueberry, tomato, orange"]
 */
@Slf4j
public class FlatMap implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		FlatMap flatMap = new FlatMap();
		flatMap.run();
	}

	private void run() {
		List<String> input = Lists.newArrayList();
		input.add("apple,orange");
		input.add("grape,apple,mango");
		input.add("blueberry,tomato,orange");

		List<String> output;

		JavaRDD<String> rdd = sc.parallelize(input);
		JavaRDD<String> result = rdd.flatMap(i -> Lists.newArrayList(i.split(",")).iterator());
		output = result.collect();

		log.warn("INPUT  : [\"apple,orange\", \"grape,apple,mango\", \"blueberry,tomato,orange\"]");
		log.warn("OUTPUT : " + output);
	}
}
