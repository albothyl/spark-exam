package com.coupang.coopers.spark.practice;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;

@Slf4j
public class CategoryCsvFile implements Serializable {

	private static final String TAG = "NEW KEYWORD RESULT :";

	public void run() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("/Users/jeonhyeong-won/Downloads/consoleFull.html");

		JavaRDD<String> categoryLog = input.filter(v1 -> StringUtils.contains(v1, TAG));

		System.out.println("categoryLog COUNT : " + categoryLog.count());
		System.out.println("categoryLog FIRST : " + categoryLog.first());


		categoryLog.persist(StorageLevel.MEMORY_ONLY());


		JavaRDD<String> csvLog = categoryLog.map(line -> line.substring(line.indexOf(TAG) + TAG.length(), line.length()));
		System.out.println("csvLog COUNT : " + csvLog.count());
		System.out.println("csvLog FIRST : " + csvLog.first());

		csvLog.saveAsTextFile("/Users/jeonhyeong-won/Downloads/spark-rdd-basic2");



        /*
         Quiz) 같이 해봐요!!

         1) ,를 줄바꿈으로 변경해서 파일로 저장하기 :
         -> CategoryCsvFileComma

         2) ":::::"로 구분해서 한줄로 변경하기 :
        -> CategoryCsvFileLine

        3) landingType별 건수 알아보기 :
        -> CategoryCsvLandingTYpe
        */
	}

	public static void main(String[] args) {
		CategoryCsvFile c = new CategoryCsvFile();
		c.run();
	}
}
