package cn.unipus.flink.e2e.sink;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author： wuxinle
 * @date： 2021/1/21 23:06
 * @description： TODO
 * @modifiedBy：
 * @version: 1.0
 */
public class TransactionTable implements Serializable {
    private transient TransactionDB db;
    private final String transactionId;
    private final List<Tuple3<String, Long, String>> buffer = new ArrayList<>();

    public TransactionTable(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public TransactionTable insert(Tuple3<String, Long, String> value) {
        initDB();
        // 投产的话，应该逻辑应该写到远端DB或者文件系统等。
        buffer.add(value);
        return this;
    }

    public TransactionTable flush() {
        initDB();
        db.firstPhase(transactionId, buffer);
        return this;
    }

    public void close() {
        buffer.clear();
    }

    private void initDB() {
        if (null == db) {
            db = TransactionDB.getInstance();
        }
    }
}
