package com.zxd.zdb.backend.server;

import com.zxd.zdb.backend.parser.Parser;
import com.zxd.zdb.backend.parser.statement.Abort;
import com.zxd.zdb.backend.parser.statement.Begin;
import com.zxd.zdb.backend.parser.statement.Commit;
import com.zxd.zdb.backend.parser.statement.Create;
import com.zxd.zdb.backend.parser.statement.Delete;
import com.zxd.zdb.backend.parser.statement.Insert;
import com.zxd.zdb.backend.parser.statement.Select;
import com.zxd.zdb.backend.parser.statement.Show;
import com.zxd.zdb.backend.parser.statement.Update;
import com.zxd.zdb.backend.tbm.BeginRes;
import com.zxd.zdb.backend.tbm.TableManager;
import com.zxd.zdb.common.Error;

public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }
    // 执行begin commit abort语句
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        // 对sql序列进行 分词 和 词法分析 返回对应的予语义类
        Object stat = Parser.Parse(sql);

        if(Begin.class.isInstance(stat)) {
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            // 事务id必须等于零，执行begin语句
            BeginRes r = tbm.begin((Begin)stat);
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            // commit 对应的xid 必须不等于零 之后 xid 置零
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            // abort 对应的xid 必须不等于零
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute2(stat);
        }
    }
    // 执行非事务相关语句
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
