package com.runone.util;


import java.util.List;

public class LocateUtils {
    private static final double EARTH_RADIUS = 6371393;

    public static double getDistance(Double lat1, Double lng1, Double lat2, Double lng2) {
        double radiansAX = Math.toRadians(lng1); // A经弧度
        double radiansAY = Math.toRadians(lat1); // A纬弧度
        double radiansBX = Math.toRadians(lng2); // B经弧度
        double radiansBY = Math.toRadians(lat2); // B纬弧度

        double cos = Math.cos(radiansAY) * Math.cos(radiansBY) * Math.cos(radiansAX - radiansBX)
                + Math.sin(radiansAY) * Math.sin(radiansBY);
        double acos = Math.acos(cos); // 反余弦值
        return EARTH_RADIUS * acos; // 最终结果
    }

    //String target_longtitude, String target_latitude, List<String> longitudes, List<String> latitudes, double max_distance

    public static boolean isInHighWayAccordLocate(String target_longtitude, String target_latitude, List<String> longitudes, List<String> latitudes) {
        Double lon = Double.valueOf(target_longtitude) / Math.pow(10, 6);
        Double lat = Double.valueOf(target_latitude) / Math.pow(10, 6);
        double[] target = LngLonUtil.gps84_To_Gcj02(lat, lon);
        int start = 0;
        int end = longitudes.size() - 1;
        int[] arr = {100, 50, 25, 10, 5, 1};
        boolean flag = false;
        for (int i = 0; i < arr.length; i++) {
            flag = false;
            int step = arr[i];
            double max_distance = step * 10;
            double min = 2000;
            int index = -1;
            for (int j = start; j <= end; j = j + step) {
                double distance = LocateUtils.getDistance(target[1], target[0], Double.valueOf(String.valueOf(longitudes.get(j))), Double.valueOf(String.valueOf(latitudes.get(j))));
                if (distance < min) {
                    min = distance;
                    index = j;
                }
            }
            if (min <= max_distance) {
                flag = true;
                start = index - step < 0 ? 0 : index - step;
                end = index + step >= end ? end : index + step;
            }
            if (!flag) {
                break;
            }
        }
        return flag;
    }


    @Deprecated
    public static boolean calculateByRange(String target_longtitude, String target_latitude, List<String> longitudes, List<String> latitudes, double max_distance) {
        Double lon = Double.valueOf(target_longtitude) / Math.pow(10, 6);
        Double lat = Double.valueOf(target_latitude) / Math.pow(10, 6);
        double[] target = LngLonUtil.gps84_To_Gcj02(lat, lon);
//        logger.info("lon:" + target[1] + "\t lat:" + target[0] + "\t the max_distance is :" + max_distance);
        for (int i = 0; i < longitudes.size(); i++) {
            double distance = LocateUtils.getDistance(target[1], target[0], Double.valueOf(String.valueOf(longitudes.get(i))), Double.valueOf(String.valueOf(latitudes.get(i))));
            if (distance <= max_distance) {
                return true;
            }
        }
        return false;
    }

}
