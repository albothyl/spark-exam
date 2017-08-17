package com.coupang.coopers.spark.basic.transformation.accumulation;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Tuple (key-value)
 * 여러개의 RDD를 같은 key를 가진 Tuple의 value를 계산하여 새로운 RDD를 생성하는데 최종 타입이 변경될 수 있다.
 * InputType은 <String, Long> 이지만 OutputType은 <String, Record> 이다.
 *
 * @Input : [(Math,100), (Eng,80), (Math,50), (Eng,90)]
 * @Output : [(Math,avg : 75), (Eng,avg : 85)]
 */
@Slf4j
public class CombineByKey implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		CombineByKey combineByKey = new CombineByKey();
		combineByKey.run();
	}

	private void run() {
		List<Tuple2<String, Long>> input = Lists.newArrayList(new Tuple2("Math", 100L), new Tuple2("Eng", 80L), new Tuple2("Math", 50L), new Tuple2("Eng", 90L));
		List<Tuple2<String, Record>> output;

		JavaPairRDD<String, Long> rdd = sc.parallelizePairs(input);
		JavaPairRDD<String, Record> result = rdd.combineByKey(
			l -> new Record(l),      //partition, key 단위, combiner가 없으면 생성
			(r, l) -> r.add(l),      //partition, key 단위, combiner가 있으면 mergeValue 적용
			(r1, r2) -> r1.add(r2)); //모든 partition의 combiner를 가져와서 적용

		output = result.collect();

		log.warn("INPUT  : " + input);
		log.warn("OUTPUT : " + output);
	}

	@Slf4j
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	private static class Record implements Serializable {
		private Long amount = 0L;
		private Long number = 0L;

		public Record(final Long amount) {
			this.amount = amount;
			this.number = 1L;
		}

		public Record add(final Record record) {
			return this.add(record.getAmount(), record.getNumber());
		}

		public Record add(final Long amount) {
			return this.add(amount, 1L);
		}

		public Record add(final Long amount, final Long number) {
			this.amount += amount;
			this.number += number;
			return this;
		}

		@Override
		public String toString() {
			final Long avg = this.amount / number;
			log.warn("amount : {}, number : {}, avg : {}", amount, number, avg);
			return "avg : " + avg;
		}
	}
}
