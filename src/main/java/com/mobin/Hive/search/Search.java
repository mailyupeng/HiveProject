package com.mobin.Hive.search;

import com.mobin.Hive.KPI.HiveDataBaseConnection;
import com.mobin.Hive.Utils.Utils;
import com.mobin.Hive.recommend.RecommendBean;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;

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


    public static void query(String g1,String b2Num, String b3Num, String startTime,String endTime,int limit) {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        StringBuffer sb = new StringBuffer();

        sb.append(" AND g1=" + g1);
        System.out.println(b2Num + "-" + b3Num);
        //判断品牌条件
        if (b2Num != null || "".equals(b2Num)) {
            String b2_condition =  "AND (array_contains(b2titleCredit," + b2Num + ") OR array_contains(b2cengCredit," + b2Num + "))";
            sb.append(b2_condition);
        }
        if (b3Num != null ||  "".equals(b3Num)) {
            String b3_condition = " AND (array_contains(b3titleCredit," + b3Num + ") OR array_contains(b3cengCredit," + b3Num + "))";
            sb.append(b3_condition);
        }



        time_condition =  " AND to_date(s6) BETWEEN " + startTime + " AND " + endTime;
        sb.append(time_condition);


        String hql = readHql(g1,sb.toString(),limit);//获取HQL
        System.out.println(hql);
        //   executeHql(hql);   //执行HQL

        if(postsNum < limit)  //不足篇数
            query1(g1,b2Num,b3Num,limit - postsNum);

        if(postsNum < limit)  //仍然不足篇数
            query2(g1,b2Num,b3Num,limit - postsNum);

        if(postsNum < limit){  //仍然不足
            query3(g1,b2Num,b3Num,limit - postsNum);
        }
    }


    //第一轮查询
    public static String readHql(String g1,String condition,int limit){
        String hql = "";
        //JDK7异常处理特性无需显示关闭IO
        String path = "HQLFile\\推荐\\重点帖子推荐.sql";
        try (FileInputStream in = new FileInputStream(path)) {
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            hql = new String(bytes).replaceAll("\\$\\{hivevar:limit\\}",String.valueOf(limit));
            hql = hql.replaceAll("\\$\\{.*\\}", condition.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hql;
    }

    //第二轮查询
    public static void query1(String g1,String b2Num, String b3Num,int limit){
        System.out.println("--------------------------");
        StringBuffer sb = new StringBuffer();
        if (b2Num != null || "".equals(b2Num)) {
            String b2_condition = " AND g1="+g1+" AND array_contains(brdint2," + b2Num + ") AND titlecredit=1 AND cengcredit=1";
            sb.append(b2_condition);
        }
        if (b3Num != null ||  "".equals(b3Num)) {
            String b3_condition = " AND g1="+g1+"  AND array_contains(brdint3," + b3Num + ") AND titlecredit=1 AND cengcredit=1";
            sb.append(b3_condition);
        }
        sb.append(time_condition);
        String hql = readHql(g1,sb.toString(),limit);//获取HQL
        System.out.println(hql);
        // executeHql(hql);   //执行HQL
    }

    //第三轮查询
    public static void query2(String g1,String b2Num, String b3Num,int limit){
        System.out.println("--------------------------");
        StringBuffer sb = new StringBuffer();
        if (b2Num != null|| "".equals(b2Num)) {
            String b2_condition = " AND g1="+g1+"  AND array_contains(brdint2," + b2Num;
            sb.append(b2_condition);
        }
        if (b3Num != null ||  "".equals(b3Num)) {
            String b3_condition = "  AND g1="+g1+" AND array_contains(brdint3," + b3Num;
            sb.append(b3_condition);
        }
        String hql = readHql1(sb.toString(),time_condition,limit);//获取HQL
        System.out.println(hql);
        // executeHql(hql);   //执行HQL
    }


     //第四轮查询
    public static void query3(String g1,String b2Num, String b3Num,int limit){
        System.out.println("--------------------------");
        StringBuffer sb = new StringBuffer();
        if (b2Num != null|| "".equals(b2Num)) {
            String b2_condition = " AND g1="+g1+"  AND array_contains(brdint2," + b2Num;
            sb.append(b2_condition);
        }
        if (b3Num != null ||  "".equals(b3Num)) {
            String b3_condition = "  AND g1="+g1+" AND array_contains(brdint3," + b3Num;
            sb.append(b3_condition);
        }
        String hql = readHql2(sb.toString(),time_condition,limit);//获取HQL
        System.out.println(hql);
        // executeHql(hql);   //执行HQL
    }


    public static String readHql1(String b2_b3_condition,String time_condition,int limit){
        String hql = "";
        //JDK7异常处理特性无需显示关闭IO
        String path = "HQLFile\\搜索\\用户导向-仍然不足篇数.sql";
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



    public static String readHql2(String b2_b3_condition,String time_condition,int limit){
        String hql = "";
        //JDK7异常处理特性无需显示关闭IO
        String path = "HQLFile\\搜索\\用户导向-仍然不足篇数1.sql";
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

    public static void executeHql(String hql,int date){
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
        Utils.transData(recommendBeen,2,date);
    }

}
