package cn.unipus.flink.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author wuxinle
 * @version 1.0
 * @date 2020/9/28 16:56
 */
public class Test2 {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStream<String> socketStream = env.socketTextStream("192.168.254.4", 9999);

    //保存迟到的数据
    OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};

    SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = socketStream
        // Time.seconds(3)有序的情况修改为0
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(3)) {
          @Override
          public long extractTimestamp(String element) {
            long eventTime = Long.parseLong(element.split(" ")[0]);
            System.out.println(eventTime);
            return eventTime;
          }
        })
        .map(new MapFunction<String, Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Long> map(String value) throws Exception {
            return Tuple2.of(value.split(" ")[1], 1L);
          }
        })
        .keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .sideOutputLateData(outputTag)
        .allowedLateness(Time.seconds(2))  //允许延迟处理2秒 触发条件为 watermark < window_end_time + lateTime
        /*
        * 11000
          12000
          23000  --当eventTime=23000到时，watermark=23000-3000=20000，触发10s-20s的窗口计算
          6> (a,2)
          15000    --因为允许数据延迟2秒，因此此时watermark=2000 < 20000+2000，继续触发计算
          6> (a,3)
          24000
          16000
          6> (a,4)
          25000   --watermark更新为25000-3000=22000
          17000   --由于watermark更新为22000，此时watermark=22000 = 20000+2000，因此这个迟到数据就不会再触发计算了。
          * */
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
          }
        });
    resultStream.print();
    DataStream<Tuple2<String, Long>> sideOutput = resultStream.getSideOutput(outputTag);
    sideOutput.print();
    env.execute();

  }
}
