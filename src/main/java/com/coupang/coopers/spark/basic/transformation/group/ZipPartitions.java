package com.coupang.coopers.spark.basic.transformation.group;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * 파티션 단위로 여러개의 RDD값에 함수를 적용하여 하나의 값으로 변환하여 RDD로 생성한다.
 *
 * @Input1 : [a, b, c]
 * @Input2 : [1, 2, 3]
 * @Output : [a1, b2, c3]
 */
@Slf4j
public class ZipPartitions implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		ZipPartitions zipPartitions = new ZipPartitions();
		zipPartitions.run();
	}

	private void run() {
		List<String> input_1 = Lists.newArrayList("a", "b", "c");
		List<Integer> input_2 = Lists.newArrayList(1, 2, 3);
		List<String> output;

		JavaRDD<String> rdd_1 = sc.parallelize(input_1, 3);
		JavaRDD<Integer> rdd_2 = sc.parallelize(input_2, 3);
		JavaRDD<String> result = rdd_1.zipPartitions(rdd_2, (i1, i2) -> {
			List<String> list = Lists.newArrayList();
			i1.forEachRemaining(s -> {
				i2.forEachRemaining(i -> list.add(s + i));
			});
			return list.iterator();
		});
		output = result.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT : " + output);
	}
}
