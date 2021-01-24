package cn.unipus.flink.e2e;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author： wuxinle
 * @date： 2021/1/19 23:06
 * @description： TODO
 * @modifiedBy：
 * @version: 1.0
 */
public class E2eExactlyOnceTestCase {

    private static Logger logger = LoggerFactory.getLogger(E2eExactlyOnceTestCase.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.MILLISECONDS) ));

        atMostOnce(env);

        env.execute("e2e-exactly-once");
    }

    /**
     *  模拟无状态的数据源，同时数据是根据时间推移而产生的，所以一旦流计算过程发生异常，
     *  那么异常期间的数据就丢失了，也就是at-most-once。
     * */
    private static void atMostOnce(StreamExecutionEnvironment env) throws Exception {
        DataStream<Tuple2<String, Long>> source = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<>("key", System.currentTimeMillis()));
                    Thread.sleep(1);
                }
            }

            @Override
            public void cancel() {

            }
        });
        
        source.map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> event) throws Exception {
                if (event.f1 % 10 == 0) {
                    String msg = String.format("Bad data [%d]...", event.f1);
                    throw new RuntimeException(msg);
                }
                return event;
            }
        }).print();
    }



}
