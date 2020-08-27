package com.runone.util;

import com.runone.bean.DataSinkInfo;
import org.ini4j.Config;
import org.ini4j.Ini;
import org.ini4j.Profile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ReadIniFile {

    public static List<DataSinkInfo> readIniFile(String iniFilepath) {
        File file = new File(iniFilepath);
        List<DataSinkInfo> dataSinks = new ArrayList<DataSinkInfo>();
        try {
            Ini ini = new Ini();
            ini.load(file);
            Set<String> keys = ini.keySet();
            keys.forEach((key) -> {
                DataSinkInfo dataSinkInfo = new DataSinkInfo();
                Profile.Section section = ini.get(key);
                String host = section.get("mq.sink.host");
                String username = section.get("mq.sink.username");
                String password = section.get("mq.sink.password");
                String virtual_host = section.get("mq.sink.virtual_host");
                String exchange = section.get("mq.sink.exchange");
                String queuename = section.get("mq.sink.queueName");
                String compute_distance = section.getOrDefault("data.highway.compute.distance","0");
                String sqlfile_path = section.get("data.highway.sqlfile");
                String streamName = section.getOrDefault("data.highway.sink.stream", key);
                String extractAll = section.getOrDefault("mq.sink.extractall", "false");
                List<String> logintitudes = new ArrayList<String>();
                List<String> latitudes = new ArrayList<String>();
                Boolean eall = Boolean.valueOf(extractAll);
                if (!eall) {
                    MysqlUtils.getHighWayInfoFromMysql(sqlfile_path, logintitudes, latitudes);
                }
                dataSinkInfo.setHost(host);
                dataSinkInfo.setUsername(username);
                dataSinkInfo.setPassword(password);
                dataSinkInfo.setCompute_distance(Double.valueOf(compute_distance));
                dataSinkInfo.setExchange(exchange);
                dataSinkInfo.setVirtual_host(virtual_host);
                dataSinkInfo.setQueuename(queuename);
                dataSinkInfo.setStreamName(streamName);
                dataSinkInfo.setLongitudes(logintitudes);
                dataSinkInfo.setLatitudes(latitudes);
                dataSinkInfo.setSectionName(key);
                dataSinkInfo.setExtract_all(eall);
                dataSinks.add(dataSinkInfo);
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataSinks;
    }

    public static void main(String[] args) {

        List<DataSinkInfo> dataSinkInfos = readIniFile("conf/datasink.ini");
        System.out.println(dataSinkInfos);

    }

}
