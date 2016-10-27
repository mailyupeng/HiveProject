package com.mobin.Hive.KPI;

import com.mobin.Hive.Utils.Utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by MOBIN on 2016/9/28.
 * 关键指_声量来源（key=2）
 */
public class Source {
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

        //TODO
        String hql ="";
        //JDK7异常处理特性无需显示关闭IO
        String num = (Num ==0) ? "日线" : (Num == 1) ? "月线" : "三-七天线";
        String path = "HQLFile\\"+num+"\\声量来源.sql";
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
        if(Num == 0 || Num == 2 || Num == 6){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,startMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,startTime);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,startMth);
            pstm.setInt(7,lastMth);
            pstm.setInt(8,startMth);
        }else if(Num == 1){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,startMth);
            pstm.setInt(3,lastMth);
            pstm.setInt(4,startMth);
        }
        ResultSet rs = pstm.executeQuery();
        List<HiveBean> hiveBean = new ArrayList();
        int i = 1;
        while(rs.next()){
            if(Num == 0 || Num == 1 || Num ==2 || (Num == 6 && (i == 1 || i % 7 == 0))) {
                HiveBean hb = new HiveBean();
                hb.setModel(rs.getString("model"));
                hb.setTime(rs.getString("time"));
                hb.setBrd3_cot(rs.getInt("brd3_cot"));
                hb.setS3a_attimg(rs.getString("s3a"));
                hb.setPer(rs.getDouble("per"));
                hb.setType(3);
                hiveBean.add(hb);
            }
            i ++;
        }

        //传输数据
        //System.out.println(JSON.toJSONString(hiveBean));
       Utils.transData(hiveBean,1,date);
        pstm.close();
        ds.close();
    }
}
