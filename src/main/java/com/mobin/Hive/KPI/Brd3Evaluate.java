package com.mobin.Hive.KPI;

import com.mobin.Hive.Utils.Utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/8.
 * 品牌总体评价(key=4)
 */
public class Brd3Evaluate {
    /*
        query函数的最后个参数
      * 日线：0
      * 三天线：2
      * 周线：6
      * 月线：1
      * */
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;
    private static String tableName = "bca";


    public static void query(int lastMth,int startMth,String lastTime,String startTime,int Num,int date) throws SQLException {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        System.out.println(lastMth + "  " + startMth);
        StringBuffer sb = new StringBuffer();
        System.out.println(Num);

        //TODO
        String hql ="";
        //JDK7异常处理特性无需显示关闭IO
        String num = (Num ==0) ? "日线" : (Num == 1) ? "月线" : "三-七天线";
        String path = "HQLFile\\"+num+"\\品牌总体评价.sql";
        try(FileInputStream in = new FileInputStream(path)){
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            String s = new String(bytes).replaceAll("\\$\\{hivevar:num\\}",String.valueOf(Num)); //三/七天线中的滑动窗口单位
            hql = s.replaceAll("\\$\\{.*\\}","?"); //hql
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }



        System.out.println(hql);
        pstm = con.prepareStatement(hql);

            pstm.setInt(1,lastMth);
            pstm.setInt(2,startMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,startTime);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,lastMth);

        ResultSet rs = pstm.executeQuery();
        List<HiveBean> hiveBean = new ArrayList();
        int i = 1;
        while(rs.next()){
            if(Num == 0 || Num == 1 || Num ==2 || (Num == 6 && (i == 1 || i % 7 == 0))) {
                HiveBean hb = new HiveBean();
                hb.setModel(rs.getString("model"));
                hb.setTime(rs.getString("time"));
                hb.setG1_cot(rs.getInt("g1_cot"));
                hb.setPer(rs.getDouble("per"));
                hb.setType(6);
                hiveBean.add(hb);
            }
            i ++;
        }

        //传输数据
       // System.out.println(JSON.toJSONString(hiveBean));
       Utils.transData(hiveBean,1,date);
        pstm.close();
        ds.close();
    }

}
