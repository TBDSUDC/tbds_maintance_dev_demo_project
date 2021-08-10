package com.tencent.tbds.utils;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

/**
 * zookeeper工具类
 */
public class ZKClient {

    private final CuratorFramework client;

    public ZKClient(String zkConnStr) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.client = CuratorFrameworkFactory.builder()
                .connectString(zkConnStr)
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        this.client.start();
    }

    public String getData(String path) throws Exception {
        byte[] data = this.client.getData().forPath(path);
        return null != data ? new String(data) : null;
    }

    public void create(String path) throws Exception {
        this.client.create().creatingParentsIfNeeded().forPath(path);
    }

    public void create(String path, String data) throws Exception {
        this.client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
    }

    public boolean pathExists(String path) throws Exception {
        Stat stat = this.client.checkExists().forPath(path);
        return stat != null;
    }

    public void setData(String path, String data) throws Exception {
        this.client.setData().forPath(path, data.getBytes());
    }

    public void close() {
        this.client.close();
    }
}
