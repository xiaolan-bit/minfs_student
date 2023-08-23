package com.ksyun.campus.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    public static void main(String args[]) throws Exception {
        //1.写数据测试
//        EFileSystem eFileSystem = new EFileSystem();
//        FSOutputStream fs = eFileSystem.create("D:/data6/ja-netfilter.jar");
//        //String s = "hello world";
//        Path path = Paths.get("D:/anzhuangfile/ja-netfilter.jar");
//        byte[] data = Files.readAllBytes(path);
//
//        //fs.write(s.getBytes());
//        fs.write(data);
//        fs.close();
        //2.读数据测试
//        EFileSystem eFileSystem = new EFileSystem();
//        FSInputStream fsInputStream = eFileSystem.open("D:/data/file4.txt");
//        byte[] buffer = new byte[1024];
//        int bytesRead;
//        int i =-1;
//        while ((bytesRead = fsInputStream.read(buffer)) != -1 && i!=0) {
//            // 处理读取到的数据
//            System.out.write(buffer, 0, bytesRead);
//            i=0;
//        }
//        // 关闭输入流
//        fsInputStream.close();
        //3.创建文件夹测试
//        EFileSystem eFileSystem = new EFileSystem();
//        eFileSystem.mkdir("D:/mkdir2/model");
        //4.删除文件测试
//        EFileSystem eFileSystem = new EFileSystem();
//        eFileSystem.delete("D:/data6/ja-netfilter.jar");
        //5.测试getClusterInfo
//        EFileSystem eFileSystem = new EFileSystem();
//        System.out.println(eFileSystem.getClusterInfo());
       //6.测试getFileStats
//        EFileSystem eFileSystem = new EFileSystem();
//        System.out.println(eFileSystem.getFileStats("/file4.txt"));
        //7.测试listFileStats
        EFileSystem eFileSystem = new EFileSystem();
        System.out.println(eFileSystem.listFileStats("/data"));
    }
}
