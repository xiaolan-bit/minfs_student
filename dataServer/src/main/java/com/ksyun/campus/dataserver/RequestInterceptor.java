package com.ksyun.campus.dataserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.servlet.http.HttpServletRequest;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class RequestInterceptor implements Interceptor {
    private HttpServletRequest request;

    public RequestInterceptor(HttpServletRequest request) {
        this.request = request;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        // 获取请求中的相关信息
        String path = request.getParameter("path");
        int offset = Integer.parseInt(request.getParameter("offset"));
        int length = Integer.parseInt(request.getParameter("length"));

        String jsonBody = "{\"path\": \"" + path + "\", \"offset\": \"" + offset + "\", \"length\": \"" + length + "\"}";

        // 读取raw数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()));
        StringBuilder rawBodyBuilder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            rawBodyBuilder.append(line);
        }
        String rawBody = rawBodyBuilder.toString();

        // 将raw数据添加到请求体中
        jsonBody += rawBody;

        // 设置自定义 Header
        String customHeaderName = "fileSystem"; // 替换为自定义 Header 名称
        String customHeaderValue = request.getHeader("fileSystem"); // 替换为实际的 Header 值

        // 构建新的请求对象
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), jsonBody);
        Request newRequest = chain.request().newBuilder()
                .addHeader(customHeaderName, customHeaderValue)
                .post(requestBody)
                .build();

        return chain.proceed(newRequest);
    }
}

