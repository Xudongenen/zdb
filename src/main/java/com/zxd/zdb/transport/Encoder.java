package com.zxd.zdb.transport;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import  com.zxd.zdb.common.Error;

public class Encoder {

    public byte[] encode(Package pkg) {
        // package 转 字节数组 标识是错误信息还是正确数据
        if(pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            // 字节数组首位是1代表有错误
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            // 首位是0代表没有错误
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    public Package decode(byte[] data) throws Exception {
        // 从字节数组 转 package
        if(data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }

}
