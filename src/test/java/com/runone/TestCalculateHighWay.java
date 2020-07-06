package com.runone;


import com.runone.util.LocateUtils;
import com.runone.util.MysqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TestCalculateHighWay {

    @Test
    public void testCalculateAlgorithm() throws IOException {
        List<String> longArr = new ArrayList<>();
        List<String> latiArr = new ArrayList<>();
        MysqlUtils.getHighWayInfoFromMysql("conf/baomao.sql", longArr, latiArr);

        //data/bmmz/2020061807352550.txt
        File file = new File("data/bmmz/2020061809364650.txt");
        FileInputStream fileInputStream = null;
        InputStreamReader isr = null;
        BufferedReader reader = null;
        try {
            fileInputStream = new FileInputStream(file);
            isr = new InputStreamReader(fileInputStream);
            reader = new BufferedReader(isr);
            String row;
            int count_v1 = 0;
            int count_v2 = 0;
            int consist = 0;
            long start_time = System.currentTimeMillis();
            while ((row = reader.readLine()) != null) {
                String[] split = row.split("\001");
                String lng = split[2];
                String lat = split[3];
                boolean value1 = LocateUtils.isInHighWayAccordLocate(lng, lat, longArr, latiArr);
                boolean value2 = LocateUtils.calculateByRange(lng, lat, longArr, latiArr, 10);
                if (value1 != value2) {
                    consist++;
                }
                if (value1) {
                    count_v1++;
                }
                if (value2) {
                    count_v2++;
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println("count_v1 总数为：" + count_v1);
            System.out.println("count_v2 总数为：" + count_v2);
            System.out.println("对比差异：" + consist);
            System.out.println("所花时间为：" + (endTime - start_time)/1000.0);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (isr != null) {
                isr.close();
            }
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }
    }

    @Test
    public void readFile2() throws IOException {
        List<String> longArr = new ArrayList<>();
        List<String> latiArr = new ArrayList<>();
        MysqlUtils.getHighWayInfoFromMysql("conf/beh.sql", longArr, latiArr);

        File file = new File("conf/2020061712434554.txt");
        FileInputStream fileInputStream = null;
        InputStreamReader isr = null;
        BufferedReader reader = null;
        try {
            fileInputStream = new FileInputStream(file);
            isr = new InputStreamReader(fileInputStream);
            reader = new BufferedReader(isr);
            String row;
            int count = 0;
            while ((row = reader.readLine()) != null) {
                String[] split = row.split("\001");
                String lng = split[2];
                String lat = split[3];
                boolean inHighWayAccordLocate = LocateUtils.calculateByRange(lng, lat, longArr, latiArr, 10);
                if (inHighWayAccordLocate) {
                    count++;
                }
            }
            System.out.println("总数为：" + count);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (isr != null) {
                isr.close();
            }
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }
    }

    /*
        data:
        15F4636078F5458773510E1CF2EBE2E40113485424231917922020-06-17 14:38:02302020-06-1714:37:2371175440000440112786627
        23DDECCB18817E1EFB927377FBCF753F0113474155232202752020-06-17 14:43:41122020-06-1714:38:2288125440000440112786435
        16027BA0B1B1832EE87394FA9831F7D50113482272232011002020-06-17 14:45:01302020-06-1714:39:1472150430000440112786627
        E6359086C6A87C7D3EAE346037935A590113413120232613002020-06-17 14:44:24302020-06-1714:39:1469299440000440111819395
        B8C69232EBB804B4E10890907F1F34810113407817232639182020-06-17 16:30:37122020-06-1716:24:4114119450000440111786947
        85B704E190289CDB4DFDA018327F441F0113268122233062222020-06-17 16:30:05302020-06-1716:24:4167294360000440111786435
        FBD0F77CFA1DBAC33349E5D0B84BB4770113337284232924832020-06-17 16:30:36122020-06-1716:24:417091450000440111786947
        7E56F7C809B4D66139EB1FE58137E6D30113507808231127482020-06-17 16:34:15302020-06-1716:29:1639193440000440112786627
        AD3DEF2738478BB9CEE765A2A99D76610113340680230913162020-06-17 16:34:20122020-06-1716:29:1600440000440105819395
        1096166AEF64483CA4FBED5C90DF55A70113585888233086662020-06-17 16:34:14112020-06-1716:29:160233440000440112786625
     */
    @Test
    public void testCalculateHighWay() {
        List<String> longArr = new ArrayList<>();
        List<String> latiArr = new ArrayList<>();
        MysqlUtils.getHighWayInfoFromMysql("conf/beh.sql", longArr, latiArr);
        Assert.assertTrue(longArr.size() != 0);
        Assert.assertTrue(latiArr.size() != 0);
        String[] lng = {"113485424", "113474155", "113482272", "113413120", "113407817", "113268122", "113337284", "113507808", "113340680", "113585888"};
        String[] lat = {"23191792", "23220275", "23201100", "23261300", "23263918", "23306222", "23292483", "23112748", "23091316", "23308666"};
        for (int i = 0; i < lng.length; i++) {
            boolean inHighWayAccordLocate = LocateUtils.isInHighWayAccordLocate(lng[i], lat[i], longArr, latiArr);
            Assert.assertEquals("经度为：" + lng[i] + "\t 维度为：" + lat[i], true, inHighWayAccordLocate);
            Assert.assertTrue(inHighWayAccordLocate);
        }
    }

    @Test
    public void testLocateUtils() {
        double distance = new LocateUtils().getDistance(113.210712621, 23.3084391701, 113.210803298, 23.3084641692);
        System.out.println(distance);
    }

}
