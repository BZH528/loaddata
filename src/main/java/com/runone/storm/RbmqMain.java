package com.runone.storm;

import com.google.gson.Gson;
import com.runone.bean.DataSinkInfo;
import com.runone.util.MysqlUtils;
import com.runone.util.ReadIniFile;
import com.runone.util.TableType;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.RabbitMQSpout;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class RbmqMain {

    public static Properties globalConfig;

    public static List<DataSinkInfo> dataSinks = new ArrayList<DataSinkInfo>();

    public static String[] getConfigFields() {
        String datatype = RbmqMain.globalConfig.getProperty("data.type", "locate");
        String[] fields;
        if (TableType.LOCATE.equalsIgnoreCase(datatype)) {
            //locate字段信息
            fields = new String[]{"VehicleNo", "PlateColorCode", "Longtitude", "Latitude", "LocateTime", "BusinessType",
                    "Speed", "DireAngle", "CarCodeLocation", "CarCodeLocationCurrent", "CarStatusCode", "routecode", "stakenum1", "stakenum2"};
        } else if (TableType.FOLLOW.equalsIgnoreCase(datatype)) {
            //follow 字段信息
            fields = new String[]{"VehicleNo", "PlateColorCode", "Longtitude", "Latitude", "LocateTime", "BusinessType", "OperatingStatus", "Speed",
                    "DireAngle", "CarCodeLocation", "CarCodeLocationCurrent", "CarStatusCode", "routecode", "stakenum1", "stakenum2"};
        } else {
            //alarm 字段信息
            fields = new String[]{"VehicleNo", "PlateColorCode", "Longtitude", "Latitude", "LocateTime", "BusinessType", "AlarmType", "OperatingStatus",
                    "Speed", "DireAngle", "CarCodeLocation", "CarCodeLocationCurrent", "CarStatusCode", "routecode", "stakenum1", "stakenum2", "encryption"};

        }
        return fields;
    }

    /**
     * 加载配置文件。
     *
     * @param config_path 配置文件的路径
     * @return 配置文件信息
     */
    private static Properties getLoadConfig(String config_path) {
        Properties proc = new Properties();
        File file = new File(config_path);
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            proc.load(fileInputStream);
            fileInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return proc;
    }

    private static void addDataSinkBolt(TopologyBuilder builder, String preBoltName) {
        List<DataSinkInfo> dataSinks = RbmqMain.dataSinks;
        dataSinks.forEach(dataSinkInfo -> {
            String sinkhost = dataSinkInfo.getHost();
            String sinkUsername = dataSinkInfo.getUsername();
            String sinkPassword = dataSinkInfo.getPassword();
            String sinkExchange = dataSinkInfo.getExchange();
            String sinkqueuename = dataSinkInfo.getQueuename();
            String sinkVirtual_host = dataSinkInfo.getVirtual_host();
            String streamName = dataSinkInfo.getStreamName();
            ConnectionConfig sinkConnectConfig = new ConnectionConfig(sinkhost, 5672, sinkUsername, sinkPassword, sinkVirtual_host, 600);
            ProducerConfig sinkConfig = new ProducerConfigBuilder().connection(sinkConnectConfig)
                    .exchange(sinkExchange)
                    .routingKey(sinkqueuename)
                    .contentEncoding("UTF-8")
                    .contentType("application/json")
                    .persistent()
                    .build();

            builder.setBolt(streamName + "_bolt", new RabbitMQBolt(new LocateTupleToMessage(globalConfig.getProperty("data.type","locate"))))
                    .addConfigurations(sinkConfig.asMap())
                    .shuffleGrouping(preBoltName, streamName);
        });

    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        Properties prop = getLoadConfig(args[0]);

        globalConfig = prop;

        Config conf = new Config();

        conf.put("dataflow.properties", prop);

        String extract = prop.getProperty("data.highway.extract", "false");
        Boolean needExtract = Boolean.valueOf(extract);
        if (needExtract) {
            String filepath = prop.getProperty("data.sink.inifile");
            dataSinks = ReadIniFile.readIniFile(filepath);
//            Gson gson = new Gson();
//            conf.put("dataSinks", gson.toJson(dataSinks));
        }

        String mq_spout_host = prop.getProperty("mq.spout.host");
        String mq_spout_username = prop.getProperty("mq.spout.username");
        String mq_spout_password = prop.getProperty("mq.spout.password");
        String mq_spout_virtual_host = prop.getProperty("mq.spout.virtual_host");
        String mq_spout_queueName = prop.getProperty("mq.spout.queueName");
        String prefetch = prop.getProperty("mq.spout.prefetch");
        String maxPending = prop.getProperty("mq.spout.maxpending");
        String filterParallism = prop.getProperty("mq.filter.parallelism", "1");
        String saveParallism = prop.getProperty("mq.save.parallelism", "1");

        ConnectionConfig spoutConnectConfig = new ConnectionConfig(mq_spout_host, 5672, mq_spout_username, mq_spout_password, mq_spout_virtual_host, 600);
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(spoutConnectConfig)
                .queue(mq_spout_queueName)
                .prefetch(Integer.valueOf(prefetch))
                .requeueOnFail()
                .build();
        RabbitMQSpout rabbitMQSpout = new RabbitMQSpout(new LocateScheme());
        builder.setSpout("rabbitMqSpout", rabbitMQSpout).addConfigurations(spoutConfig.asMap()).setMaxSpoutPending(Integer.valueOf(maxPending));

        builder.setBolt("highwayFilter", new LocateDataFilter(dataSinks, getConfigFields()), Integer.parseInt(filterParallism)).shuffleGrouping("rabbitMqSpout");

        addDataSinkBolt(builder, "highwayFilter");

        builder.setBolt("hdfsbolt", new SaveDataBolt(prop), Integer.parseInt(saveParallism)).shuffleGrouping("highwayFilter", "common");

        if (args != null && args.length > 1) {
            //集群模式运行
            conf.setNumWorkers(1);
            try {

                String toponame;
                if (args.length >= 2) {
                    toponame = args[1];
                } else {
                    toponame = prop.getProperty("storm.cluster.toponame", "loadData_" + new Random(100).nextInt());
                }

                StormSubmitter.submitTopology(toponame, conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            //本地模式运行
            try {
                LocalCluster localModeRun = new LocalCluster();
                conf.setMaxTaskParallelism(1);
                localModeRun.submitTopology("DataFromMqLocalMode", conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
