package com.mobin.Hive.recommend;

import com.alibaba.fastjson.JSON;
import com.mobin.Hive.KPI.HiveBean;
import com.mobin.Hive.KPI.HiveDataBaseConnection;
import com.mobin.Hive.Utils.Utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/25.
 */
public class G1Recommend {
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;

    public static void query(String startTime,int date) {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(startTime));

        int startMth = Utils.disMonth(calendar);
        int lastMth = 0;
        String lastTime = "";

        if(date == 1){
            lastMth = startMth;
            lastTime = startTime;
        }else if(date == 2){//三天线
            calendar.add(Calendar.DATE, -2);
            lastTime = new Date(calendar.getTime().getTime()).toString();
            lastMth = Utils.disMonth(calendar);
        }else if(date == 3){  //周线
            calendar.add(Calendar.DATE, -6);
            lastTime = new Date(calendar.getTime().getTime()).toString();
            lastMth = Utils.disMonth(calendar);
        }else { //月线
            calendar.roll(Calendar.MONTH, false);
            lastTime = new Date(calendar.getTime().getTime()).toString();
            lastMth = Utils.disMonth(calendar);
        }

        //TODO
        String hql ="";
        //JDK7异常处理特性无需显示关闭IO
        String path = "HQLFile\\推荐\\重点网友推荐.sql";
        try(FileInputStream in = new FileInputStream(path)){
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            hql = new String(bytes).replaceAll("\\$\\{.*\\}","?"); //三/七天线中的滑动窗口单位
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println(hql);
        try {
            pstm = con.prepareStatement(hql);
            pstm.setInt(1,lastMth);
            pstm.setInt(2,startMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,startTime);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,startMth);
            pstm.setString(7,lastTime);
            pstm.setString(8,startTime);
            ResultSet rs = pstm.executeQuery();
            List<G1Bean> g1Beens = new ArrayList();
            int i = 1;
            while(rs.next()){
                G1Bean g1 = new G1Bean();
                g1.setG1(rs.getString("g1"));
                g1.setS2(rs.getString("s2"));
                g1.setS8a(rs.getString("s8a"));
                g1Beens.add(g1);
            }

            //传输数据
             //Utils.transData(g1Beens,2,date);
            pstm.close();
            ds.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }





    }
}
