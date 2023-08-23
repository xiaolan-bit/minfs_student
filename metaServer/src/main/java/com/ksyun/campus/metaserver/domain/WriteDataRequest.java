package com.ksyun.campus.metaserver.domain;

import lombok.Data;

@Data
public class WriteDataRequest {
    private String fileSystem;
    private String path;
    private int offset;
    private int length;
    private byte[] requestBody;

}
