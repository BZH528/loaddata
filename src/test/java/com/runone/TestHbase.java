package com.runone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestHbase {

    private static final String ZKconnect = "192.168.10.102:2181,192.168.10.103:2181,192.168.10.104:2181";

    private Configuration conf;

    private String tableName = "trafficinfo";

    private String familyName = "location";

    @Before
    public void setup() {
        this.conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZKconnect);
    }

    @Test
    public void testCreateTable() {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
//            HColumnDescriptor location = new HColumnDescriptor("location");
            desc.addFamily(new HColumnDescriptor(familyName));
            // 为提高效率，对hbase表进行预分区
//            0,1,2,3,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z
            String[] splits = new String[]{"3", "6|", "9|", "A|", "D|", "G|", "J|", "M|", "P|", "S|", "V|", "X|", "Z"};
            byte[][] keys = new byte[splits.length][];
            for (int i = 0; i < splits.length; i++) {
                keys[i] = Bytes.toBytes(splits[i]);
            }
            if (admin.tableExists(Bytes.toBytes(tableName))) {
                System.out.println("表已经存在！");
            } else {
                admin.createTable(desc, keys);
                System.out.println("成功创建！");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutRecord() {
        HTable htable = null;
        try {
            htable = new HTable(conf, tableName);
            String rowKey = "2EAEA873E2A7FC3B96C57401649C9F99";
            String logitude = "116566360";
            String latitude = "23639289";
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("logitude"), Bytes.toBytes(logitude));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("latitude"), Bytes.toBytes(latitude));
            htable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGet() {
        String rowKey = "2EAEA873E2A7FC3B96C57401649C9F99";
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable htable = null;
        try {
            htable = new HTable(conf, Bytes.toBytes(tableName));
            Result result = htable.get(get);
            List<Cell> cells = result.listCells();
            List<KeyValue> column = result.getColumn(Bytes.toBytes("location"), Bytes.toBytes("logitude"));
            column.forEach(f -> {
                System.out.println(f);
            });
            for (int i = 0; i < cells.size(); i++) {
                Cell cell = cells.get(i);
                String name = new String(cell.getQualifier());
                String value = new String(cell.getValue());
                System.out.println("name:" + name + "\t" + "value:" + value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() {
        HTable htable = null;
        try {
            String rowKey = "2EAEA873E2A7FC3B96C57401649C9F99";
            htable = new HTable(conf, tableName);
            Delete de = new Delete(Bytes.toBytes(rowKey));
            htable.delete(de);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdate() {
        HTable htable = null;
        try {
            String rowKey = "2EAEA873E2A7FC3B96C57401649C9F99";
            htable = new HTable(conf, Bytes.toBytes(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(familyName), Bytes.toBytes("logitude"), Bytes.toBytes("333333333"));
            put.add(Bytes.toBytes(familyName), Bytes.toBytes("latitude"), Bytes.toBytes("333333333"));
            htable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCapability() {
        File file = new File("file/2020081815265652.txt");
        long totalTime = 0;
        int count = 0;
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(fileInputStream);
            BufferedReader reader = new BufferedReader(isr);
            String line;
            HTable htable = new HTable(conf, tableName);
            long start = System.currentTimeMillis();
            List<Put> list = new ArrayList<Put>();
//            ExecutorService executorService = Executors.newFixedThreadPool(10);
            while ((line = reader.readLine()) != null) {
                String[] split = line.split("\001");
                String rowkey = split[0];
                String logitude = split[2];
                String latitude = split[3];
                if (split.length < 5) continue;
                String locatetime = split[4];
                Put put = new Put(Bytes.toBytes(rowkey));
                put.setWriteToWAL(false);
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("logitude"), Bytes.toBytes(logitude));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("latitude"), Bytes.toBytes(latitude));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("locatetime"), Bytes.toBytes(locatetime));
                list.add(put);
                if (list.size() >= 500) {
//                    WorkThread workThread = new WorkThread(htable, list);
////                    workThread.join();
////                    executorService.submit(workThread);
                    htable.put(list);
                    list = new ArrayList<Put>();
//                    int i = Thread.activeCount();
//                    System.out.println("=============================>" + i);
                }
                count++;
//                System.out.println("当前数据条数为：" + count);
            }
            long end = System.currentTimeMillis();
            totalTime = end - start;
            reader.close();
            isr.close();
            fileInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("总时间为:" + totalTime);
        System.out.println("数据总条数为:" + count);
    }

    @Test
    public void testDeleteTable() {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}

//测试多线程提交
//class WorkThread extends Thread {
//
//    private List<Put> datalist;
//    private HTable htable;
//
//    public WorkThread(HTable htable, List<Put> datalist) {
//        this.htable = htable;
//        this.datalist = datalist;
//    }
//
//    @Override
//    public void run() {
//        try {
//            this.htable.put(this.datalist);
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("出现错误！");
//        }
//        System.out.println("==================" + Thread.currentThread().getName());
//    }
//}