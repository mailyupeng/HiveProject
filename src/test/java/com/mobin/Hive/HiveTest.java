package com.mobin.Hive;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;



import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class HiveTest{




   public static  void jdbc(String str,String ... s) {
       System.out.println(str.length() + "  " + s.length);

//        Calendar calendar = Calendar.getInstance();
//        calendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2016-04-4"));
//        int day = calendar.get(Calendar.DAY_OF_WEEK);
//        calendar.add(calendar.DATE,-168);
//
//        String s = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
//      //  calendar.add(calendar.DATE,-6);
////        String s1 = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
//        System.out.println(s);
//
////        System.out.println(new java.sql.Date(calendar.getTime().getTime()));
////        System.out.println(java.sql.Date.valueOf("2016-01-01"));
//
//
//        String str = "SELECT model,time,sum(cot) over(ORDER BY time DESC ROWS BETWEEN CURRENT ROW and 2 FOLLOWING) AS cot FROM" +
//                " (SELECT model,time,count(DISTINCT g1) AS cot FROM " +
//                " (SELECT sid,g1,cbrdint3 AS model FROM " + "bca" +
//                " LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE cbrdint3=? AND (mth>=? AND mth<=?)) t1" +
//                " JOIN (SELECT rowkey,to_date(s6) AS time FROM raws WHERE (mth>=? AND mth<=?) AND to_date(s6)>=? AND  to_date(s6)<=?) t2" +
//                " ON base64(t1.sid)=base64(substr(t2.rowkey,0,16))" +
//                " GROUP BY model,time) tmp";
//
//        StringBuffer sb = new StringBuffer();
//        sb.append("SELECT model,time,sum(cot) over(ORDER BY time DESC ROWS BETWEEN CURRENT ROW and 2 FOLLOWING) AS cot FROM(");
//        sb.append("SELECT model,time,count(DISTINCT g1) AS cot FROM " +
//                " (SELECT sid,g1,cbrdint3 AS model FROM " + "bca" +
//                        " LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE cbrdint3=? AND (mth>=? AND mth<=?)) t1" +
//                        " JOIN (SELECT rowkey,to_date(s6) AS time FROM raws WHERE (mth>=? AND mth<=?) AND to_date(s6)>=? AND  to_date(s6)<=?) t2" +
//                        " ON base64(t1.sid)=base64(substr(t2.rowkey,0,16))" +
//                        " GROUP BY model,time");
//        sb.append(") tmp");
//        String hql = sb.toString();
//
//
//
//
//
////        Properties properties = new Properties();
////        properties.getProperty("");
//
//
//
//        //  (calendarNow.get(Calendar.YEAR) - calendarBirth.get(Calendar.YEAR))* 12+ calendarNow.get(Calendar.MONTH) - calendarBirth.get(Calendar.MONTH);
//
//HiveBean bean = new HiveBean();
////        bean.setModel("pp");
////        String ss = JSON.to;
////        System.out.println(ss);





    }


    public static void main(String[] args) {
        String json = "{'A':'2'}";


    }

}
