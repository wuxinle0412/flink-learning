package cn.unipus.flink.e2e.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.CheckpointListener;

/**
 * @author： wuxinle
 * @date： 2021/1/21 23:01
 * @description： TODO
 * @modifiedBy：
 * @version: 1.0
 */
public class MapFunctionWithException extends
        RichMapFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>>
        implements CheckpointListener {

    private long delay;
    private transient volatile boolean needFail = false;

    public MapFunctionWithException(long delay) {
        this.delay = delay;
    }

    @Override
    public Tuple3<String, Long, String> map(Tuple3<String, Long, String> event) throws Exception {
        Thread.sleep(delay);
        if(needFail){
            throw new RuntimeException("Error for testing...");
        }
        return event;
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        this.needFail = true;
        System.err.println(String.format("MAP - CP SUCCESS [%d]", l));
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {

    }
}
