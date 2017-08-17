package com.coupang.coopers.spark.basic.action.output;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * RDD에서 모든 데이터에 대해서 지정한 연산을 수행하여 다른 타입의 값으로 출력한다.
 *
 * @Input : [100, 80, 75, 90, 95]
 * @Output : 88
 */
@Slf4j
public class Aggregate implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		Aggregate aggregate = new Aggregate();
		aggregate.run();
	}

	private void run() {
		List<Long> input = Lists.newArrayList(100L, 80L, 75L, 90L, 95L);
		Record output;
		Record initRecord = new Record(0L, 0L);

		JavaRDD<Long> rdd = sc.parallelize(input);
		output = rdd.aggregate(initRecord, (r, v) -> r.add(v), (r1, r2) -> r1.add(r2));

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
