package com.mobin.Hive;

import com.alibaba.fastjson.JSON;


import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class Commentators {

    /*
     query函数的最后个参数
   * 日线：0
   * 三天线：2
   * 周线：6
   * 月线：1
   * */
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private PreparedStatement pstm;
    private static String tableName = "bca";

    //（日线）每天关注该车系的人数,时间轴为30天
    public  void dayLine(String model,String thisTime) throws SQLException, IOException, ParseException {
        //根据开始时间推出上个月的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int thisMth = disMonth(calendar);   //这个月的编号
        int lastMth = thisMth-1;            //上个月的编号

        calendar.roll(Calendar.MONTH,false);   //上滚一个月
        String lastTime = new Date(calendar.getTime().getTime()).toString();//上个月的时间

        query(model,lastMth,thisMth,lastTime,thisTime,1,0);
    }

    //（三天线）时间轴为31天内30个“三天线”数据
    public void threeDayLine(String model,String thisTime) throws ParseException, SQLException {
        //根据开始时间推出三天前的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));
        int thisMth = disMonth(calendar);   //今天对应的mth编号

        calendar.add(Calendar.DATE,-32);   //上滚三天
        String lastTime = new Date(calendar.getTime().getTime()).toString();//三天前的时间
        int lastMth = disMonth(calendar);            //三天前的mth编号
        System.out.println(thisMth);
        System.out.println(lastMth);

        query(model,lastMth,thisMth,lastTime,thisTime,2,2);
    }


    //时间轴为过去24周的周数据
    public void weekLine(String model,String thisTime) throws SQLException, ParseException {
        //根据开始时间推出24周前的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int day = calendar.get(Calendar.DAY_OF_WEEK);//这周的第几天
        calendar.add(Calendar.DATE,-day);           //上滚N天
        String WeekLastDay = new Date(calendar.getTime().getTime()).toString();//上周的最后一天时间
        int WeekLastDayMth = disMonth(calendar);   //上周的最后一天对应的mth编号

        calendar.add(Calendar.DATE,-168);
        String Before24WeekDay = new Date(calendar.getTime().getTime()).toString();//24周前的第一天
        int Before24WeekDayMth = disMonth(calendar);            //24周前的第一天对应的mth编号

        query(model,Before24WeekDayMth,WeekLastDayMth,Before24WeekDay,WeekLastDay,3,6);
    }

    //时间轴为过去24个月的月数据
    public void monthLine(String model, String thisTime) throws ParseException, SQLException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int thisMth = disMonth(calendar) - 1;   //这个月的编号
        int lastMth = thisMth - 24;            //前24个月的编号
        query(model,lastMth,thisMth,"","",4,1);
    }


    public void query(String model,int lastMth,int thisMth,String lastTime,String thisTime, int type,int Num) throws SQLException {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        System.out.println(lastMth + "  " + thisMth);
        StringBuffer sb = new StringBuffer();

        String s1 = "";
        String s2 = "";
        String s3 = "";
        if(Num == 2 || Num == 6 || Num == 0){
            if(Num == 2 || Num == 6){
                s1 = "SELECT model,time,sum(cot) over(ORDER BY time DESC ROWS BETWEEN CURRENT ROW and "+Num+" FOLLOWING) AS cot FROM(";
                s2 = ") tmp";
            }
            s3 = "SELECT model,time,count(DISTINCT g1) AS cot FROM " +
                    " (SELECT sid,g1,cbrdint3 AS model FROM " + tableName +
                    " LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a==1 OR s9!=1) AND  cbrdint3=? AND mth>=? AND mth<=?) t1" +
                    " JOIN (SELECT rowkey,to_date(s6) AS time FROM raws WHERE (s3a==1 OR s9!=1) AND  mth>=? AND mth<=? AND to_date(s6)>=? AND  to_date(s6)<=?) t2" +
                    " ON base64(t1.sid)=base64(substr(t2.rowkey,0,16))" +
                    " GROUP BY model,time";
        }
        if(Num == 1){
            System.out.println(11);
            s3 = "SELECT model,time,count(DISTINCT g1) AS cot FROM " +
                    " (SELECT sid,g1,cbrdint3 AS model,mth AS time FROM " + tableName +
                    " LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a==1 OR s9!=1) AND  cbrdint3=? AND mth>=? AND mth<=?) t1" +
                    " GROUP BY model,time";
        }

        String hql = s1 + s3 + s2;

        pstm = con.prepareStatement(hql);
        pstm.setString(1,model);
        pstm.setInt(2,lastMth);
        pstm.setInt(3,thisMth);
        pstm.setInt(4,lastMth);
        pstm.setInt(5,thisMth);
        pstm.setString(6,lastTime);
        pstm.setString(7,thisTime);
        ResultSet rs = pstm.executeQuery();
        List<HiveBean> hiveBean = new ArrayList();
        int i = 1;
        while(rs.next()){
            if(Num == 0 || Num == 1 || Num ==2 || (Num == 6 && (i == 1 || i % 7 == 0))) {
                HiveBean hb = new HiveBean();
                hb.setModel(rs.getString("model"));
                hb.setTime(rs.getString("time"));
                hb.setCot(rs.getInt("cot"));
                hb.setType(type);
                hiveBean.add(hb);
            }
            i ++;
        }

        transData(hiveBean);
        pstm.close();
        con.close();
    }


    //计算两个年份相差多少个月
    public static int disMonth(Calendar nowTime) throws ParseException {
        Calendar startTime = Calendar.getInstance();
        startTime.setTime(Date.valueOf("1999-12-01"));
        return (nowTime.get(Calendar.YEAR) - startTime.get(Calendar.YEAR))* 12 +
               nowTime.get(Calendar.MONTH) - startTime.get(Calendar.MONTH);
    }

    //传输数据
    public static void transData(List<HiveBean> bean){
        String hiveJSON = JSON.toJSONString(bean);
        System.out.println(hiveJSON);
//        String urlStr = "http://data.dgchina.com/xingji/Handle/GetData.aspx?model="+hiveJSON;
//        URL url;
//        try {
//            url = new URL(urlStr);
//            URLConnection URLconnection = url.openConnection();
//            HttpURLConnection httpConnection = (HttpURLConnection)URLconnection;
//            httpConnection.setDoOutput(true);
//            httpConnection.setChunkedStreamingMode(1024*1024);
//            httpConnection.setRequestMethod("POST");
//
//            DataOutputStream out = new DataOutputStream(httpConnection.getOutputStream());
//            out.writeUTF(hiveJSON);
//            out.flush();
//            out.close();
//        } catch (MalformedURLException e) {
//            e.printStackTrace();
//        } catch (ProtocolException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public static void main(String[] args) throws SQLException, IOException, ParseException {
        Commentators h = new Commentators();
        h.dayLine("162","2016-04-01");
       // h.threeDayLine("162","2016-04-04");
        //h.weekLine("162","2016-04-04");
       // h.monthLine("162","2016-04-04");
       /* Calendar calendar = Calendar.getInstance();
        calendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2016-04-30"));
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("1999-12-01"));
        System.out.println(h.disMonth(calendar));*/

    }
}
