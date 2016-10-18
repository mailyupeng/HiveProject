package com.mobin.Hive.Utils;

import com.alibaba.fastjson.JSON;
import com.mobin.Hive.HiveBean;
import jodd.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.sql.Date;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;

/**
 * Created by MOBIN on 2016/9/28.
 */
public class Utils {
    //数据传输
    public static void transData(List<HiveBean> bean){
        System.out.println(bean.size() + "  size");
        String model = "&model="+JSON.toJSONString(bean) + "&date=1";  //date

        System.out.println(model);
        String urlStr = "http://data.dgchina.com/xingji/Handle/GetData.aspx";
        URL url;
        try {
            url = new URL(urlStr);
            URLConnection URLconnection = url.openConnection();
            HttpURLConnection httpConnection = (HttpURLConnection)URLconnection;
            httpConnection.setDoOutput(true);
            httpConnection.setChunkedStreamingMode(model.getBytes().length);
            httpConnection.setRequestMethod("POST");

            DataOutputStream out = new DataOutputStream(httpConnection.getOutputStream());
            out.write(model.getBytes());
            out.flush();
            out.close();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //计算两个年份相差多少个月
    public static int disMonth(Calendar nowTime) throws ParseException {
        Calendar startTime = Calendar.getInstance();
        startTime.setTime(Date.valueOf("1999-12-01"));
        return (nowTime.get(Calendar.YEAR) - startTime.get(Calendar.YEAR))* 12 +
                nowTime.get(Calendar.MONTH) - startTime.get(Calendar.MONTH);
    }
}
