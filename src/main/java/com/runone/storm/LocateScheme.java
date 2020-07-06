package com.runone.storm;

import com.runone.util.TableType;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LocateScheme implements Scheme {

    private String datatype;

    public LocateScheme() {
        this.datatype = RbmqMain.globalConfig.getProperty("data.type", "locate");
    }

    public List<Object> deserialize(ByteBuffer byteBuffer) {
        String msg = "";
        try {
            msg = new String(byteBuffer.array(), "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        List<String> datas = Arrays.asList(msg.substring(1, msg.length() - 1).split(","));

        ArrayList<Object> res = new ArrayList<>();
        res.addAll(datas);

        // 由于升级需要，需要新增三个字段，兼容尚未升级成功的数据个数。
        if (TableType.ALARM.equalsIgnoreCase(datatype)) {
            if (res.size() < 14) {
                res.add("");
                res.add("");
                res.add("");
                res.add("");
            }
        } else if (TableType.FOLLOW.equalsIgnoreCase(datatype)) {
            if (res.size() < 14) {
                res.add("");
                res.add("");
                res.add("");
            }
        } else {
            if (res.size() < 14) {
                res.add("");
                res.add("");
                res.add("");
            }
        }

        return res;
    }

    public Fields getOutputFields() {
        String[] fields = RbmqMain.getConfigFields();
        return new Fields(fields);
    }

}
