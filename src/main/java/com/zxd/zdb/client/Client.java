package com.zxd.zdb.client;

import  com.zxd.zdb.transport.Package;
import  com.zxd.zdb.transport.Packager;

public class Client {
    // 具体的发送接收
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        // 进行编码
        Package pkg = new Package(stat, null);
        // 发送 得到返回结果
        Package resPkg = rt.roundTrip(pkg);
        // 判断返回有没有错误
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        // 返回执行结果
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
