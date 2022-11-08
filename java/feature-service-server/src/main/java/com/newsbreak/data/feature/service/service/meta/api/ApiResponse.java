package com.newsbreak.data.feature.service.service.meta.api;

/**
 * @projectName: data-feature-service
 * @package: com.newsbreak.data.feature.service.service.meta
 * @className: MetaApiResponse
 * @author: Yifan ZHANG
 * @description:
 * @date: 2022/10/18 15:00
 * @version: 1.0
 */
public class ApiResponse<T> {

    private int code;

    private String msg;

    private T data;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
