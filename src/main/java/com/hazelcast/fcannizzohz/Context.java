package com.hazelcast.fcannizzohz;

public record Context(String clusterName, String memberAddress) {
    public static Context DEV_LOCALHOST = new Context("dev", "localhost:5701");
}
