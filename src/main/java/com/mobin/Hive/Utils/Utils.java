package com.mobin.Hive.Utils;

import com.alibaba.fastjson.JSON;
import com.mobin.Hive.KPI.HiveBean;

import java.io.*;
import java.net.*;
import java.sql.Date;
import java.text.ParseException;
import java.util.*;

/**
 * Created by MOBIN on 2016/9/28.
 */
public class Utils {
    //数据传输
    //type:1表示指标，2表示推荐，3表示搜索
    /**
     * @param bean
     * @param type:表示是指标组，推荐组和搜索组
     * @param date:表示X线
     * */
    public static void transData(List bean,int type,int date){

        System.out.println(bean.size() + "  size");
        System.out.println(date + "8888");
        String url = null;
        if(type == 1){
            url = "http://data.dgchina.com/xingji/Handle/GetData.aspx";
        }else if(type == 2){
            url = "http://data.dgchina.com/xingji/Handle/GetRecommendData.aspx";
        }

        //http://data.dgchina.com/xingji/Handle/GetRecommendData.aspx

//        HashMap<String,String> map = new HashMap<String, String>();
//        map.put("date", "1");
//        map.put("modelJson", JSON.toJSONString(bean));
        //String s = upload(urlStr, map, null, null);
       // System.out.println(s);
        //分开传数据，一次2万条
        int size = bean.size();
        int fromIndex = 0;
        int toIndex = 0;
        int times = size/20000 -1;
        String rs = null;
        if(size > 20000){
            for(int i = 0; i <= times; i ++){
                if(i == times)
                    toIndex = size -1;
                else
                    toIndex = (i+1)*20000;
                rs = Utils.sendPost(url,"&modelJson="+JSON.toJSONString(bean.subList(fromIndex,toIndex))+"&date=" + String.valueOf(date));
                fromIndex = toIndex;
            }
            System.out.println(rs);
        }else {
             rs = Utils.sendPost(url,"&modelJson="+JSON.toJSONString(bean)+"&date=" + String.valueOf(date));
             System.out.println(rs);
        }
       // System.out.println(JSON.toJSONString(bean));

/*
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
        }*/
    }
    //计算两个年份相差多少个月
    public static int disMonth(Calendar nowTime){
        Calendar startTime = Calendar.getInstance();
        startTime.setTime(Date.valueOf("1999-12-01"));
        return (nowTime.get(Calendar.YEAR) - startTime.get(Calendar.YEAR))* 12 +
                nowTime.get(Calendar.MONTH) - startTime.get(Calendar.MONTH);
    }



    public static String upload(String url, HashMap<String,String> values, String fileName, byte[] postBytes) {
            try {
                String boundary = "------WebKitFormBoundary22f3e68d32a54b36f";
                URL u = new URL(url);
                HttpURLConnection conn = (HttpURLConnection) u.openConnection();

                conn.setDoOutput(true);           // 设置是否向httpUrlConnection输出
                conn.setDoInput(true);            // 设置是否从httpUrlConnection读入，默认情况下是true;
                conn.setUseCaches(false);        // Post 请求不能使用缓存
                conn.setRequestMethod("POST");
                conn.setRequestProperty("connection", "Keep-Alive");
                conn.setRequestProperty("Charsert", "UTF-8");
                conn.setRequestProperty("Content-Type", "multipart/form-data;boundary=" + boundary);
                //http://my.oschina.net/u/994934/blog/181610
                //if (Build.VERSION.SDK != null && Build.VERSION.SDK_INT > 13) {
                //    conn.setRequestProperty("Connection", "close"); //sdk3.2之后用这个
                //}else{
                //    conn.setRequestProperty("http.keepAlive", "false"); // sdk2.2之前用这个
                //}
                //在JDK 1.5以后可以这样来设置超时时间
                conn.setConnectTimeout(30000);
                conn.setReadTimeout(30000);
                // 指定流的大小，当内容达到这个值的时候就把流输出
                conn.setChunkedStreamingMode(10240);
                OutputStream out = new DataOutputStream(conn.getOutputStream());
                byte[] end_data = ("\r\n--" + boundary + "--\r\n").getBytes();// 定义最后数据分隔线
                //List<String> list  = new ArrayList<String>();
                //list.add("e:/userInfo.properties");
                //list.add("e:/email.html");
                //values.put("qin", "22f3e68dflybird32a54b36f");
                Iterator<String> iterator = values.keySet().iterator();
                StringBuilder sb = new StringBuilder();
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    //添加form属性
                    sb.append("--");
                    sb.append(boundary);
                    sb.append("\r\n");
                    sb.append("Content-Disposition: form-data; name=\"");
                    sb.append(key);
                    sb.append("\"\r\n\r\n");
                    sb.append(values.get(key));
                    sb.append("\r\n");
                }
                out.write(sb.toString().getBytes("utf-8"));
                //out.write("\r\n".getBytes("utf-8"));
                if (postBytes != null) {
                    sb.delete(0, sb.length());
                    sb.append("--");
                    sb.append(boundary);
                    sb.append("\r\n");
                    sb.append("Content-Disposition: form-data;name=\"file\";filename=\"" + fileName + "\"\r\n");
                    sb.append("Content-Type:application/octet-stream\r\n\r\n");
                    byte[] data = sb.toString().getBytes();
                    out.write(data);
                    for (int i = 0; i < postBytes.length; i += 1024) {
                        if (i + 1024 < postBytes.length) {
                            out.write(postBytes, i, 1024);
                        } else {
                            out.write(postBytes, i, postBytes.length - i);
                        }
                    }
                }
                //out.write("\r\n".getBytes()); // 多个文件时，二个文件之间加入这个
                out.write(end_data);
                out.flush();
                out.close();
                // 定义BufferedReader输入流来读取URL的响应
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String line = null;
                //String reline = null;
                StringBuilder sb2 = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                    //reline = line;
                    sb2.append(line);
                    sb2.append("\n");
                }
                //return reline;
                return sb2.toString();
            } catch (Exception e) {
                //System.out.println("发送POST请求出现异常！" + e);
                e.printStackTrace();
            }

        return null;
    }


    public static String sendGet(String url, String param) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url + "?" + param;
            URL realUrl = new URL(urlNameString);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.setReadTimeout(3600);
            connection.setConnectTimeout(3600);
            // 建立实际的连接
            connection.connect();
            // 获取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 遍历所有的响应头字段
            for (String key : map.keySet()) {
                System.out.println(key + "--->" + map.get(key));
            }
            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 向指定 URL 发送POST方法的请求
     *
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }
        return result;
    }


}
