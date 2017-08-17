package com.coupang.coopers.spark.practice;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.coupang.coopers.spark.practice.PracticeUtil.findSpec;
import static com.coupang.coopers.spark.practice.PracticeUtil.findSrc;
import static com.coupang.coopers.spark.practice.PracticeUtil.getClickEventDTOList;

@Slf4j
public class ClickEventPractice implements Serializable {

	private static final Integer CLICK_COUNT = 1;

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		ClickEventPractice clickEventPractice = new ClickEventPractice();
		clickEventPractice.run();
	}

	private void run() {
		List<ClickEventDTO> input = getClickEventDTOList();
		Map<String, Long> output;

		JavaRDD<ClickEventDTO> rdd = sc.parallelize(input);
		JavaRDD<String> srcRdd = rdd.map(c -> findSrc(c.getSrc()) + ".");
		JavaRDD<String> specRdd = rdd.map(c -> findSpec(c.getSpec()) + ".ClickCount");

		output = srcRdd
			.zip(specRdd)
			.map(m -> m._1 + m._2)
			.countByValue();

		for (Map.Entry<String, Long> metricEntry : output.entrySet()) {
			log.warn(metricEntry.getKey() + " : " + metricEntry.getValue());
		}
	}
}
