package com.zxd.zdb.backend.dm;

import com.zxd.zdb.backend.common.AbstractCache;
import com.zxd.zdb.backend.dm.dataItem.DataItem;
import com.zxd.zdb.backend.dm.dataItem.DataItemImpl;
import com.zxd.zdb.backend.dm.logger.Logger;
import com.zxd.zdb.backend.dm.page.Page;
import com.zxd.zdb.backend.dm.page.PageOne;
import com.zxd.zdb.backend.dm.page.PageX;
import com.zxd.zdb.backend.dm.pageCache.PageCache;
import com.zxd.zdb.backend.dm.pageIndex.PageIndex;
import com.zxd.zdb.backend.dm.pageIndex.PageInfo;
import com.zxd.zdb.backend.tm.TransactionManager;
import com.zxd.zdb.backend.utils.Panic;
import com.zxd.zdb.backend.utils.Types;
import com.zxd.zdb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // entry 转 di
        byte[] raw = DataItem.wrapDataItemRaw(data);

        // 判断di 大小不能超过页面大小
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }
        // 选取页面
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            // 找合适的页面
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 没找到创建新的页面
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        // 找不到合适的页面 也创建不了新的页面
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            // 根据页面信息 取到具体页面
            pg = pc.getPage(pi.pgno);
            // 先写日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 写具体页面
            short offset = PageX.insert(pg, raw);
            // 内存中释放页面
            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex（可能已经变化了剩余空间）
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
    
}
