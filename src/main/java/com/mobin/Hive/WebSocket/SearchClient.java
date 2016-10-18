package com.mobin.Hive.WebSocket;

import com.alibaba.fastjson.JSON;

import javax.websocket.*;
import java.net.URI;

/**
 * Created by MOBIN on 2016/10/13 .
 */
@ClientEndpoint
public class SearchClient {

    private String sender;
    private String time;
    private String message;

    static class Message{
        protected String user;
        protected String model;
        protected String time;
        protected int brd3_cot;
        protected int g1_cot;
        protected String s3a_attimg;
        protected double per;
        protected double per1;
        protected int type;
    }


    @OnOpen
    public void onOpen(Session session) {
        System.out.println("Connected to endpoint: " + session.getBasicRemote());
    }

    @OnMessage
    public void onMessage(String message) {
        SearchClient searchBean = JSON.parseObject(message,SearchClient.class);
        if(!searchBean.sender.equals("mobin")){  //判断是否是自己的发送信息
            Message ms = JSON.parseObject(searchBean.message,Message.class); //客户端发送过来的message JSON是查询所需的数据
            System.out.println(ms.model);
            System.out.println(message+ "message");
            System.out.println(searchBean.sender);
            StringBuffer sb = new StringBuffer();
            //HQL
            if(ms.user != null)
                sb.append(" AND g1=" + ms.user);
            String modelCondition = ms.model != null ? (" AND model=" + ms.model) : ("默认的值");  //model条件
            sb.append(modelCondition);
            String hql = "";
        }

    }

    @OnError
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    public static void websocketTest() {
        try {
                    WebSocketContainer container = ContainerProvider.getWebSocketContainer(); // 获取WebSocket连接器，其中具体实现可以参照websocket-api.jar的源码,Class.forName("org.apache.tomcat.websocket.WsWebSocketContainer");
                    String uri = "ws://web.dgchina.com:8080/itrac/ws/mobin";
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
}