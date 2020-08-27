package com.runone.bean;

import java.io.Serializable;
import java.util.List;

public class DataSinkInfo implements Serializable {

    private String host;
    private String username;
    private String password;
    private String virtual_host;
    private String exchange;
    private String queuename;
    private String sectionName;

    private String streamName;

    private double compute_distance;

    private boolean extract_all;

    public boolean isExtract_all() {
        return extract_all;
    }

    public void setExtract_all(boolean extract_all) {
        this.extract_all = extract_all;
    }

    private List<String> longitudes;
    private List<String> latitudes;

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getVirtual_host() {
        return virtual_host;
    }

    public void setVirtual_host(String virtual_host) {
        this.virtual_host = virtual_host;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getQueuename() {
        return queuename;
    }

    public void setQueuename(String queuename) {
        this.queuename = queuename;
    }

    public double getCompute_distance() {
        return compute_distance;
    }

    public void setCompute_distance(double compute_distance) {
        this.compute_distance = compute_distance;
    }

    public List<String> getLongitudes() {
        return longitudes;
    }

    public void setLongitudes(List<String> longitudes) {
        this.longitudes = longitudes;
    }

    public List<String> getLatitudes() {
        return latitudes;
    }

    public void setLatitudes(List<String> latitudes) {
        this.latitudes = latitudes;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }
}
