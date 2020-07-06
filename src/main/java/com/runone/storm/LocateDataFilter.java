package com.runone.storm;

import com.runone.bean.DataSinkInfo;
import com.runone.util.LngLonUtil;
import com.runone.util.LocateUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LocateDataFilter implements IRichBolt {

    private OutputCollector outputCollector;

    private List<DataSinkInfo> dataSinkInfos;

    private Logger logger;

    private final String[] outputFileds;

    public LocateDataFilter(List<DataSinkInfo> dataSinkInfos, String[] outputFields) {
        this.dataSinkInfos = dataSinkInfos;
        this.outputFileds = outputFields;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        //从context里面读取配置文件数据，初始化excutor
//        this.dataSinkInfos = new ArrayList<>();
//        String dataStr = (String) context.getConf().get("dataSinks");
//        Gson gson = new Gson();
//        JsonArray jsonArr = new JsonParser().parse(dataStr).getAsJsonArray();
//        for (JsonElement jsonElement : jsonArr) {
//            dataSinkInfos.add(gson.fromJson(jsonElement, DataSinkInfo.class));
//        }

//        JSONObject jsonObject = (JSONObject) context.getConf().get("dataflow.properties");
//        String s = jsonObject.get("mq.spout.password");

//        this.dataSinkInfos = RbmqMain.dataSinks;


        this.logger = LoggerFactory.getLogger(LocateDataFilter.class);
    }

    public void execute(Tuple tuple) {
        String longtitude = tuple.getStringByField("Longtitude");
        String latitude = tuple.getStringByField("Latitude");
        List<Object> fields = tuple.getValues();

        boolean flag = false;
        Iterator<DataSinkInfo> iterator = dataSinkInfos.iterator();

        //提取高速数据
        while (iterator.hasNext()) {
            DataSinkInfo datainfo = iterator.next();
            logger.info("to alalyze highway：" + datainfo.getSectionName());
            if (isInHighWay(longtitude, latitude, datainfo.getLongitudes(), datainfo.getLatitudes(), datainfo.getCompute_distance())) {
                flag = true;
                this.logger.info("---------------------------it is in highway-----------------------------");
                this.outputCollector.emit(datainfo.getStreamName(), fields);
            }
        }

        if (!flag) {
            this.logger.info("############################it is in not highway#########################");
        }

        this.outputCollector.emit("common", fields);

        this.outputCollector.ack(tuple);
    }


    private boolean isInHighWay(String target_longtitude, String target_latitude, List<String> longitudes, List<String> latitudes, double max_distance) {
        return LocateUtils.isInHighWayAccordLocate(target_longtitude, target_latitude, longitudes, latitudes);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        String[] fields = RbmqMain.getConfigFields();
        String[] fields = this.outputFileds;
//        Iterator<DataSinkInfo> iterator = RbmqMain.dataSinks.iterator();
        Iterator<DataSinkInfo> iterator = this.dataSinkInfos.iterator();
        while (iterator.hasNext()) {
            DataSinkInfo datainfo = iterator.next();
            outputFieldsDeclarer.declareStream(datainfo.getStreamName(), new Fields(fields));
        }
        outputFieldsDeclarer.declareStream("common", new Fields(fields));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
