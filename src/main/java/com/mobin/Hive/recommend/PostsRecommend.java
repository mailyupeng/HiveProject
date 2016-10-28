package com.mobin.Hive.recommend;

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
public class PostsRecommend {
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;
    private static int postsNum = 0;  //帖子数量
    private static String time_condition;

    /**
     * @param date：时间条件为1-日线/2-三天线/3-周线/4-月线
     * @param brandNum                      :品牌编号
     * @param b2
     * @param b3
     * @param startTime                     :开始时间
     * @param limit                         :文章数
     */
    public static void query(int date, int brandNum, String b2, String b3, String startTime,int limit) {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        StringBuffer sb = new StringBuffer();

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(Date.valueOf(startTime));
            int startMth = Utils.disMonth(calendar);

            //判断品牌条件
            if (b2.equals("b2")) {
                String b2_condition = " AND (array_contains(b2titleCredit," + brandNum + ") OR array_contains(b2cengCredit," + brandNum + "))";
                sb.append(b2_condition);
            }
            if (b3.equals("b3")) {
                String b3_condition = " AND (array_contains(b3titleCredit," + brandNum + ") OR array_contains(b3cengCredit," + brandNum + "))";
                sb.append(b3_condition);
            }

            //判断X线
            if (date == 1) {
                time_condition =  " AND mth= " + startMth + " AND to_date(s6)=" + startTime;
            } else if (date == 2) {
                calendar.add(Calendar.DATE, -2);
                String lastTime = new Date(calendar.getTime().getTime()).toString();
                int lastMth = Utils.disMonth(calendar);
                time_condition = " AND mth BETWEEN " + startMth + " AND " + lastMth +
                                 " AND to_date(s6) BETWEEN " + startTime + " AND " + lastTime ;
            } else if (date == 3) {
                calendar.add(Calendar.DATE, -6);
                String lastTime = new Date(calendar.getTime().getTime()).toString();
                int lastMth = Utils.disMonth(calendar);
                time_condition = " AND mth BETWEEN " + startMth + " AND " + lastMth +
                                 " AND to_date(s6) BETWEEN " + startTime + " AND " + lastTime;
            }
            else { //月线
                calendar.roll(Calendar.MONTH, false);
                int lastMth = Utils.disMonth(calendar);
                time_condition = " AND mth BETWEEN " + startMth + " AND " + lastMth;
            }
            sb.append(time_condition);


        String hql = readHql(sb.toString(),limit);//获取HQL
        System.out.println(hql);
     //   executeHql(hql);   //执行HQL

        if(postsNum < limit)  //不足篇数
            query1(brandNum,b2,b3,limit - postsNum,date);

        if(postsNum < limit)  //仍然不足篇数
            query2(brandNum,b2,b3, limit - postsNum,date);
    }

    public static void query1(int brandNum, String b2, String b3,int limit,int date){
        System.out.println("--------------------------");
        StringBuffer sb = new StringBuffer();
        if (b2.equals("b2")) {
            String b2_condition = " AND array_contains(brdint2," + brandNum + ") AND titlecredit=1 AND cengcredit=1";
            sb.append(b2_condition);
        }
        if (b3.equals("b3")) {
            String b3_condition = " AND array_contains(brdint3," + brandNum + ") AND titlecredit=1 AND cengcredit=1";
            sb.append(b3_condition);
        }
        sb.append(time_condition);
        String hql = readHql(sb.toString(),limit);//获取HQL
        System.out.println(hql);
        // executeHql(hql);   //执行HQL
    }

    public static void query2(int brandNum, String b2, String b3,int limit,int date){
        System.out.println("--------------------------");
        StringBuffer sb = new StringBuffer();
        if (b2.equals("b2")) {
            String b2_condition = " AND array_contains(brdint2," + brandNum;
            sb.append(b2_condition);
        }
        if (b3.equals("b3")) {
            String b3_condition = " AND array_contains(brdint3," + brandNum;
            sb.append(b3_condition);
        }
        String hql = readHql1(sb.toString(),time_condition,limit);//获取HQL
        System.out.println(hql);
        // executeHql(hql);   //执行HQL
    }


    public static String readHql(String condition,int limit){
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
