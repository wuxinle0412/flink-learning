package cn.unipus.flink.watermark;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author wuxinle
 * @version 1.0
 * @date 2020/9/28 16:09
 */
public class Test1 {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);

    DataStream<Tuple2<String, Long>> resultStream = env.addSource(new SourceFunction<Tuple2<String, Long>>() {

      @Override
      public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        ctx.collect(Tuple2.of("a", 10000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("b", 12000L));  //落入0-10S窗口
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 20000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 21000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("b", 22000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 23000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 24000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("b", 29000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 30000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 22000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 23000L));
        Thread.sleep(5);
        ctx.collect(Tuple2.of("a", 33000L));

      }

      @Override
      public void cancel() {

      }
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(3)) {
      @Override
      public long extractTimestamp(Tuple2<String, Long> element) {
        System.out.println("eventTime: " + element.f1);
        return element.f1;
      }
    }).keyBy(item -> item.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
          }
        });

    resultStream.print();

    env.execute("watermark-test");
  }
}
