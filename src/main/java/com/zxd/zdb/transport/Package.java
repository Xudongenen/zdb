package  com.zxd.zdb.transport;

public class Package {
    // 将数据+错误信息进行包装包装
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
