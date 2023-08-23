package com.ksyun.campus.client;

import org.apache.hc.client5.http.classic.HttpClient;
import org.springframework.web.client.RestTemplate;

public abstract class FileSystem {
    private String fileSystem;
    private static HttpClient httpClient;
    private RestTemplate restTemplate;
    protected void callRemote(){
//        httpClient.execute();
    }
    public void EFileSystem() {
        restTemplate = new RestTemplate();
    }


}
