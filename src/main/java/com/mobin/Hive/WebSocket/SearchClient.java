package com.mobin.Hive.WebSocket;

import com.alibaba.fastjson.JSON;
import com.google.common.annotations.VisibleForTesting;
import com.mobin.Hive.search.Search;

import javax.websocket.*;
import java.net.URI;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/13 .
 */
@ClientEndpoint
public class SearchClient {

    private String sender;
    private String time;
    private String message;


    static class Message{
        public String key;
        public String startTime;
        public String endTime;
        public List<Author> authorList;
        public List<Keyword> keyWordList;
        public String model;
        public String time;
        public int brd3_cot;
        public int g1_cot;
        public String s3a_attimg;
        public double per;
        public double per1;
        public int type;


        @Override
        public String toString() {
            return "Message{" +
                    "key='" + key + '\'' +
                    ", startTime='" + startTime + '\'' +
                    ", endTime='" + endTime + '\'' +
                    ", authorList=" + authorList +
                    ", keyWordList=" + keyWordList +
                    ", model='" + model + '\'' +
                    ", time='" + time + '\'' +
                    ", brd3_cot=" + brd3_cot +
                    ", g1_cot=" + g1_cot +
                    ", s3a_attimg='" + s3a_attimg + '\'' +
                    ", per=" + per +
                    ", per1=" + per1 +
                    ", type=" + type +
                    '}';
        }
    }

    static class Keyword{
        public String type;
        public String keyWord;
    }
    static class Author{
        public String author;
    }

    @OnOpen
    public void onOpen(Session session) {
        System.out.println("Connected to endpoint: " + session.getBasicRemote());
    }

    @OnMessage
    public void onMessage(String message) {
        System.out.println(message);
        String b2Num = null;
        String b3Num = null;
        String g1 = null;
        SearchClient searchBean = JSON.parseObject(message,SearchClient.class);
        if(!searchBean.sender.equals("self")){  //判断是否是自己的发送信息
            Message ms = JSON.parseObject(searchBean.message,Message.class); //客户端发送过来的message JSON是查询所需的数据

            //拼接查询条件
            StringBuffer condition = new StringBuffer();
            if(ms.authorList.size()>=1){   //有g1
                List<Author> authors = ms.authorList;
                       g1 = authors.get(0).author;
                for(Keyword keywords: ms.keyWordList) {
                    if (keywords.type.equals("B2")) {
                        b2Num = keywords.keyWord;
                    } else if (keywords.type.equals("B3")) {
                        b3Num = keywords.keyWord;
                    }

                }
                Search.query(g1,b2Num,b3Num,ms.startTime,ms.endTime,20);
            }else{  //没有g1
                for(Keyword keywords: ms.keyWordList){
                    if(keywords.type.equals("B2")){
                        b2Num = keywords.keyWord;
                    }else if(keywords.type.equals("B3")){
                        b3Num = keywords.keyWord;
                    }
                    else if(keywords.type.equals("C")){
                        condition.append(" AND c6=" + keywords.keyWord);
                    }else if(keywords.type.equals("A")){
                        condition.append(" AND A4=" + keywords.keyWord);
                    }else if(keywords.type.equals("FM")){
                        condition.append(" AND fm=" + keywords.keyWord);
                    }
                }
            }
            //B2对应品牌   B3对应车系
            //condition.append(" AND to_date(s6) BETWEEN " + "2016-04-01"+ " AND " + "2016-04-28");
        }

    }

    @OnError
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    public static void websocketTest() {
        try {
                    WebSocketContainer container = ContainerProvider.getWebSocketContainer(); // 获取WebSocket连接器，其中具体实现可以参照websocket-api.jar的源码,Class.forName("org.apache.tomcat.websocket.WsWebSocketContainer");
                    String uri = "ws://web.dgchina.com:8080/itrac/ws/self";
                    Session session = container.connectToServer(SearchClient.class, new URI(uri)); // 连接会话
                    int count = 1;
                    while (true) {
                        session.getBasicRemote().sendText("\"测试" + count++ + "\""); // 发送文本消息
                        Thread.sleep(5000);
                    }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SearchClient.websocketTest();
    }


    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}