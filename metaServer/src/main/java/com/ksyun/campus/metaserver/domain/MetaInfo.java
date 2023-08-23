package com.ksyun.campus.metaserver.domain;

import lombok.Data;

@Data
public class MetaInfo {
    private String ip;
    private int port;
    private String role;
}
