package com.coupang.coopers.spark.basic.transformation.aggregation;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Tuple (key-value)
 * 두 RDD_1, RDD_2를 같은 key로 묶지만 같은 key가 없더라도 Optional.empty로 Tuple을 추가하여 새로운 RDD를 생성한다.
 *
 * @Input1 : [(a,1), (b,1), (c,1)]
 * @Input2 : [(b,2), (c,2)]
 * @Output (LeftOuterJoin) : [(a,(1,Optional.empty)), (b,(1,Optional[2])), (c,(1,Optional[2]))]
 * @Output (RightOuterJoin) : [(b,(Optional[1],2)), (c,(Optional[1],2))]
 */
@Slf4j
public class OuterJoin implements Serializable {

	final SparkConf conf = new SparkConf().setMaster("local").setAppName("Test").set("spark.driver.host", "localhost");
	final JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		OuterJoin outerJoin = new OuterJoin();
		outerJoin.run();
	}

	private void run() {
		List<Tuple2<String, Integer>> input_1 = Lists.newArrayList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("c", 1));
		List<Tuple2<String, Integer>> input_2 = Lists.newArrayList(new Tuple2("b", 2), new Tuple2("c", 2));
		List<Tuple2<String, Tuple2<Integer, Optional<Integer>>>> output_1;
		List<Tuple2<String, Tuple2<Optional<Integer>, Integer>>> output_2;

		JavaPairRDD<String, Integer> rdd_1 = sc.parallelizePairs(input_1);
		JavaPairRDD<String, Integer> rdd_2 = sc.parallelizePairs(input_2);
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> result_1 = rdd_1.leftOuterJoin(rdd_2);
		JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> result_2 = rdd_1.rightOuterJoin(rdd_2);
        output_1 = result_1.collect();
		output_2 = result_2.collect();

		log.warn("INPUT 1 : " + input_1);
		log.warn("INPUT 2 : " + input_2);
		log.warn("OUTPUT (LeftOuterJoin) : " + output_1);
		log.warn("OUTPUT (RightOuterJoin) : " + output_2);
	}
}
