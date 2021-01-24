package cn.unipus.flink.e2e.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.UUID;

/**
 * @author： wuxinle
 * @date： 2021/1/21 23:07
 * @description： TODO
 * @modifiedBy：
 * @version: 1.0
 *
 * 功能描述: 端到端的精准一次语义sink示例（测试）
 * TwoPhaseCommitSinkFunction有4个方法:
 * - beginTransaction() Call on initializeState
 * - preCommit() Call on snapshotState
 * - commit()  Call on notifyCheckpointComplete()
 * - abort() Call on close()
 */
public class E2EExactlyOnceSinkFunction extends TwoPhaseCommitSinkFunction<Tuple3<String, Long, String>, TransactionTable, Void> {

    public E2EExactlyOnceSinkFunction() {
        super(new KryoSerializer<>(TransactionTable.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(TransactionTable table, Tuple3<String, Long, String> value, Context context) throws Exception {
        table.insert(value);
    }

    /**
     * Call on initializeState
     */
    @Override
    protected TransactionTable beginTransaction() {
        return TransactionDB.getInstance().createTable(
                String.format("TransID-[%s]", UUID.randomUUID().toString()));
    }

    /**
     * Call on snapshotState
     */
    @Override
    protected void preCommit(TransactionTable table) throws Exception {
        table.flush();
        table.close();
    }

    /**
     * 两阶段提交函数中，将会在notifyCheckpointComplete()中调用，即完成checkpoint后，将数据真正提交到外部系统
     * Call on notifyCheckpointComplete()
     */
    @Override
    protected void commit(TransactionTable table) {
        System.err.println(String.format("SINK - CP SUCCESS [%s]", table.getTransactionId()));
        TransactionDB.getInstance().secondPhase(table.getTransactionId());
    }

    /**
     * Call on close()
     */
    @Override
    protected void abort(TransactionTable table) {
        TransactionDB.getInstance().removeTable("Abort", table.getTransactionId());
        table.close();
    }
}
