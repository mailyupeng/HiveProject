package com.mobin.Hive.search;

import com.mobin.Hive.KPI.HiveDataBaseConnection;
import com.mobin.Hive.Utils.Utils;
import com.mobin.Hive.recommend.RecommendBean;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/26.
 */
public class Search {
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;
    private static int postsNum = 0;  //帖子数量
    private static String time_condition;


    public static void query(String condition,int limit) {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();



        String hql = readHql(condition,limit);//获取HQL
        System.out.println(hql);
        //   executeHql(hql);   //执行HQL

        //if(postsNum < 5)  //不足篇数
           // query1(condition);

       // if(postsNum < 5)  //仍然不足篇数
          //  query2(condition);
    }




    public static String readHql(String condition,int limit){
        String hql = "";
        //JDK7异常处理特性无需显示关闭IO
        String path = "HQLFile\\搜索\\用户.sql";
        try (FileInputStream in = new FileInputStream(path)) {
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            hql = new String(bytes).replaceAll("\\$\\{hivevar:limit\\}",String.valueOf(limit));
            hql = hql.replaceAll("\\$\\{.*\\}", condition);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hql;
    }



    public static String readHql1(String b2_b3_condition,String time_condition,int limit){
        String hql = "";
        //JDK7异常处理特性无需显示关闭IO
        String path = "HQLFile\\推荐\\重点帖子推荐-仍然不足篇.sql";
        try (FileInputStream in = new FileInputStream(path)) {
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            hql = new String(bytes).replaceAll("\\$\\{hivevar:limit\\}",String.valueOf(limit));
            hql = hql.replaceAll("\\$\\{hivevar:brdint3\\}",b2_b3_condition);
            hql = hql.replaceAll("\\$\\{hivevar:time\\}",time_condition);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hql;
    }

    public static void executeHql(String hql){
        List<RecommendBean> recommendBeen = new ArrayList<>();
        try {
            pstm = con.prepareStatement(hql);
            ResultSet rs = pstm.executeQuery();
            while(rs.next()){
                RecommendBean bean = new RecommendBean();
                bean.setS4(rs.getString("s4"));
                bean.setS6(rs.getString("s6"));
                bean.setS1(rs.getString("s1"));
                bean.setS2(rs.getString("a_s2"));
                bean.setQ1(rs.getString("q1"));
                postsNum ++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
       // Utils.transData(recommendBeen,"search");
    }
}
