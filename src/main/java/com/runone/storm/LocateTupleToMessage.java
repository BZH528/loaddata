package com.runone.storm;

import io.latent.storm.rabbitmq.TupleToMessageNonDynamic;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.UnsupportedEncodingException;

public class LocateTupleToMessage extends TupleToMessageNonDynamic {
    protected byte[] extractBody(Tuple tuple) {
        String[] fileds = {"VehicleNo", "PlateColorCode", "BusinessType", "CarCodeLocation", "CarCodeLocationCurrent", "LocateTime", "Longtitude", "Latitude",
                "Speed", "DireAngle", "alarm", "CarStatusCode", "linecode", "warn"};
        String[] data = new String[fileds.length];
        for (int i = 0; i < fileds.length; i++) {
            if (i == 10 || i == 12 || i == 13) {
                data[i] = "0";
                continue;
            }
            String v = tuple.getStringByField(fileds[i]);
            data[i] = v;
        }
        String join = String.join(",", data);
        try {
            return join.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

}
