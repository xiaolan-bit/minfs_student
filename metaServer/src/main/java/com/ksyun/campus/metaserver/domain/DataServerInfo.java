package com.ksyun.campus.metaserver.domain;

import lombok.Data;

@Data
public class DataServerInfo {
    private String ip;
    private int port;
    private String capacity;
    private String rack;
    private String zone;
    private int fileTotal;

    // 构造函数、getter和setter方法等
    // ...
}
