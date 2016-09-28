package com.mobin.Hive;

import com.alibaba.fastjson.JSON;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.List;

/**
 * Created by MOBIN on 2016/9/28.
 */
public class TransData {
    public static void transData(List<HiveBean> bean){
        String hiveJSON = JSON.toJSONString(bean);
        System.out.println(hiveJSON);
        String urlStr = "http://data.dgchina.com/xingji/Handle/GetData.aspx?model="+hiveJSON;
        URL url;
        try {
            url = new URL(urlStr);
            URLConnection URLconnection = url.openConnection();
            HttpURLConnection httpConnection = (HttpURLConnection)URLconnection;
            httpConnection.setDoOutput(true);
            httpConnection.setChunkedStreamingMode(hiveJSON.getBytes().length);
            httpConnection.setRequestMethod("POST");

            DataOutputStream out = new DataOutputStream(httpConnection.getOutputStream());
            out.writeUTF(hiveJSON);
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
}
