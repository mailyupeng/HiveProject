package com.mobin.Hive;

import com.mobin.Hive.Utils.Utils;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by MOBIN on 2016/9/28.
 * 关键指_声量份额
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
    private PreparedStatement pstm;
    private static String tableName = "bca";

    //（日线）每天关注该车系的人数,时间轴为30天
    public  void dayLine(String model,String thisTime) throws SQLException, IOException, ParseException {
        //根据开始时间推出上个月的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int thisMth = Utils.disMonth(calendar);   //这个月的编号
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
        int thisMth = Utils.disMonth(calendar);   //今天对应的mth编号

        calendar.add(Calendar.DATE,-32);   //上滚三天
        String lastTime = new Date(calendar.getTime().getTime()).toString();//三天前的时间
        int lastMth = Utils.disMonth(calendar);            //三天前的mth编号
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
        int WeekLastDayMth = Utils.disMonth(calendar);   //上周的最后一天对应的mth编号

        calendar.add(Calendar.DATE,-168);
        String Before24WeekDay = new Date(calendar.getTime().getTime()).toString();//24周前的第一天
        int Before24WeekDayMth = Utils.disMonth(calendar);            //24周前的第一天对应的mth编号

        query(model,Before24WeekDayMth,WeekLastDayMth,Before24WeekDay,WeekLastDay,3,6);
    }

    //时间轴为过去24个月的月数据
    public void monthLine(String model, String thisTime) throws ParseException, SQLException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int thisMth = Utils.disMonth(calendar) - 1;   //这个月的编号
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
        String s4 = "";
        String s5 = "";
        String s6 = "";
        String s7 = "";
        if(Num == 2 || Num == 6 || Num == 0){

            s1 = "WITH wtmp AS (SELECT rowkey,to_date(s6) AS time,cx FROM raws WHERE (s3a==1 OR s9!=1) AND  (mth>=? AND mth<=?) AND to_date(s6)>=? AND to_date(s6)<=?),cx_tmp AS";
            if(Num == 2 || Num == 6){
                s2 = "(SELECT s3a,time,sum(c) over(PARTITION BY s3a ORDER BY time DESC rows between CURRENT row and "+Num+" following) AS cx_per FROM " +
                     "(SELECT count(1) AS c,time,s3a FROM ";
                s4 = "t2) " +
                       " SELECT model,cx_tmp.time,s3a,cx_per,brdint3_cot,(cx_per/brdint3_cot) AS precents FROM" +
                       " (SELECT model,time,cot,(sum(cot) over(ORDER BY time DESC rows between CURRENT row and "+Num+" following)) AS brdint3_cot FROM ";
                s6 = ")t5 ";
            }else{  //Num == 0
                s2 = "(SELECT s3a,time,count(1) AS s_cot FROM ";
                s4 = "SELECT model,t5.time,cot AS brdint3_cot,s3a,(s_cot/cot) AS percents FROM";
            }
                s3 = "(SELECT sid,s3a FROM bca " +
                     "WHERE (s3a==1 OR s9!=1) AND   mth>=? AND mth<=? AND array_contains(brdint3,?) AND s3a!=4) t1 " +
                     "JOIN wtmp ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) GROUP BY s3a,time) ";

                s5 = "(SELECT model,time,count(1) AS cot FROM " +
                     "(SELECT sid,cbrdint3 AS model FROM bca " +
                     "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a==1 OR s9!=1) AND  cbrdint3=? AND (mth>=? AND mth<=?)) t3 " +
                     "JOIN wtmp " +
                     "ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) " +
                     "GROUP BY model,time) t5 ";

                s7 = "JOIN cx_tmp ON (cx_tmp.time=t5.time)";
        }
        if(Num == 1){
            System.out.println(11);
            s3 = "WITH cx_tmp AS " +
                 "(SELECT s3a,count(1) AS c,mth AS time " +
                 "FROM bca WHERE (s3a==1 OR s9!=1) AND  mth>=? AND mth<=? AND array_contains(brdint3,?) GROUP BY mth,s3a) " +
                 "SELECT model,t5.time,cot AS brdint3_cot,c,(c/cot) AS percents FROM " +
                 "(SELECT model,time,count(1) AS cot FROM " +
                 "(SELECT sid,cbrdint3 AS model,mth AS time FROM bca " +
                 "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a==1 OR s9!=1) AND  cbrdint3=? AND mth>=? AND mth<=?) t1 " +
                 "GROUP BY model,time) t5 " +
                 "JOIN cx_tmp ON(cx_tmp.time=t5.time)";
        }

        String hql = s1 + s2 + s3 + s4 + s5 + s6 + s7;

        System.out.println(hql);
        pstm = con.prepareStatement(hql);
        if(Num == 0 || Num == 2 || Num == 6){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,thisMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,thisTime);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,thisMth);
            pstm.setString(7,model);
            pstm.setString(8,model);
            pstm.setInt(9,lastMth);
            pstm.setInt(10,lastMth);
        }else if(Num == 1){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,thisMth);
            pstm.setString(3,model);
            pstm.setString(4,model);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,thisMth);
        }
        ResultSet rs = pstm.executeQuery();
        List<HiveBean> hiveBean = new ArrayList();
        int i = 1;
        while(rs.next()){
            if(Num == 0 || Num == 1 || Num ==2 || (Num == 6 && (i == 1 || i % 7 == 0))) {
                HiveBean hb = new HiveBean();
                hb.setModel(rs.getString("model"));
                hb.setTime(rs.getString("time"));
                hb.setCot(rs.getInt("brdint3_cot"));
                hb.setPercent(rs.getDouble("percents"));
                hb.setType(type);
                hiveBean.add(hb);
            }
            i ++;
        }

        //传输数据
        Utils.transData(hiveBean);
        pstm.close();
        con.close();
    }

    public static void main(String[] args) throws SQLException, IOException, ParseException {
        Source source = new Source();
        source.dayLine("162","2016-04-01");
        // h.threeDayLine("162","2016-04-04");
        //h.weekLine("162","2016-04-04");
       // source.monthLine("162","2016-04-04");

    }
}
