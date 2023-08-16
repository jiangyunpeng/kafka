package com.buzz.kafka.test;

public enum Env {
    TEST("172.16.48.179:9081,172.16.48.180:9081,172.16.48.181:9081"),
    DRC("172.16.48.182:9011,172.16.48.182:9012,172.16.48.183:9011"),
    LOG("172.16.49.6:9093,172.16.49.12:9093,172.16.49.10:9093");
    private String servers;

    Env(String servers) {
        this.servers = servers;
    }

    public String getServers() {
        return servers;
    }
}
