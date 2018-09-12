package com.xian.liankloud.jdbc;

import java.io.File;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;

/**
 * @Title: ReadTxtFile.java
 * @Description:
 * @author: hangbin.zhang
 * @date 2018年9月6日
 * @version 1.0
 */
public class ReadTxtFile {
    @SuppressWarnings("unused")
    public static void main(String[] args) {
        try {

            /* 读入TXT文件 */
            String pathname = "D:\\data\\shopping\\output\\shopping\\part-00001"; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径
            File filename = new File(pathname);
            InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
            BufferedReader br = new BufferedReader(reader);
            String line = "bg";
            while (line != null) {

                line = br.readLine(); // 一次读入一行数据

                if(line == null ) {
                    break;
                }else if(line.isEmpty() || line.equals("bg")) {
                    continue;
                }
                System.out.println(line);

                String rs =  line.replace("(", "").replace(")", "");
                String[] ary = rs.split(",");
                String name1= ary[0];
                String name2= ary[1];
                double value= Double.parseDouble(ary[2]);

                InsertDB insertData = new InsertDB();

                insertData.saveData(name1,name2,value);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
