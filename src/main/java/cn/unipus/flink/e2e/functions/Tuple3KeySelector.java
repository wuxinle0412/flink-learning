package cn.unipus.flink.e2e.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author： wuxinle
 * @date： 2021/1/21 21:33
 * @description： TODO
 * @modifiedBy：
 * @version: 1.0
 */
public class Tuple3KeySelector implements KeySelector<Tuple3<String, Long, String>, String> {
    @Override
    public String getKey(Tuple3<String, Long, String> event) throws Exception {
        return event.f0;
    }
}
