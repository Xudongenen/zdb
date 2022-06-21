package com.zxd.zdb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import com.zxd.zdb.backend.tm.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    // 事务id
    public long xid;
    // 事务隔离级别
    public int level;
    // 所有事务快照
    public Map<Long, Boolean> snapshot;
    // 异常信息
    public Exception err;
    // 自动回滚
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 如果不是读提交（也就是可重复读级别）
        if(level != 0) {
            // 可重读时，将所有当前活跃事务都设置为运行
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
