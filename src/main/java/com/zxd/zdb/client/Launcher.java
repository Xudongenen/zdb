package com.zxd.zdb.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import  com.zxd.zdb.transport.Encoder;
import  com.zxd.zdb.transport.Packager;
import  com.zxd.zdb.transport.Transporter;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        // 客户端socket
        Socket socket = new Socket("127.0.0.1", 9999);
        // 编码sql模块
        Encoder e = new Encoder();
        // 传输模块
        Transporter t = new Transporter(socket);
        // 具体进行打包和传输
        Packager packager = new Packager(t, e);
        // 建立数据库客户端
        Client client = new Client(packager);
        // 建立交互shell
        Shell shell = new Shell(client);
        shell.run();
    }
}
