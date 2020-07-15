package com.runone.storm;

import com.runone.util.TableType;
import io.latent.storm.rabbitmq.TupleToMessageNonDynamic;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class LocateTupleToMessage extends TupleToMessageNonDynamic {

    private String dataType;

    public LocateTupleToMessage(String dataType) {
        super();
        this.dataType = dataType;
    }

    protected byte[] extractBody(Tuple tuple) {
        String[] fields;
        if (TableType.LOCATE.equalsIgnoreCase(this.dataType)) {
            fields = new String[]{"VehicleNo", "PlateColorCode", "BusinessType", "CarCodeLocation", "CarCodeLocationCurrent", "LocateTime", "Longtitude", "Latitude",
                    "Speed", "DireAngle", "alarm", "CarStatusCode", "linecode", "warn"};
        } else if (TableType.ALARM.equalsIgnoreCase(this.dataType)) {
            fields = new String[]{"VehicleNo", "PlateColorCode", "encryption", "Longtitude", "Latitude", "LocateTime", "BusinessType", "AlarmType", "OperatingStatus",
                    "Speed", "DireAngle", "CarCodeLocation", "CarCodeLocationCurrent", "CarStatusCode", "routecode", "stakenum1", "stakenum2"};
        } else {
            fields = new String[]{"VehicleNo", "PlateColorCode", "BusinessType", "CarCodeLocation", "CarCodeLocationCurrent", "LocateTime", "Longtitude", "Latitude",
                    "Speed", "DireAngle", "alarm", "CarStatusCode", "linecode", "warn"};
        }

        String[] data = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            if (i == 10 || i == 12 || i == 13) {
                data[i] = "0";
                continue;
            }
            String v = tuple.getStringByField(fields[i]);
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
