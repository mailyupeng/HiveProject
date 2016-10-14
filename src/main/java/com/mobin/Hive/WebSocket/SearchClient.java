package com.mobin.Hive.WebSocket;

import javax.websocket.*;
import java.net.URI;

/**
 * Created by MOBIN on 2016/10/13 .
 */
@ClientEndpoint
public class SearchClient {

    @OnOpen
    public void onOpen(Session session) {
        System.out.println("Connected to endpoint: " + session.getBasicRemote());
    }

    @OnMessage
    public void onMessage(String message) {
        System.out.println(message);
    }

    @OnError
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    public static void websocketTest() {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer(); // 获取WebSocket连接器，其中具体实现可以参照websocket-api.jar的源码,Class.forName("org.apache.tomcat.websocket.WsWebSocketContainer");
            String uri = "ws://web.dgchina.com:8080/itrac/ws/test.java";
            Session session = container.connectToServer(SearchClient.class, new URI(uri)); // 连接会话
            int count = 1;
            while (true) {
                session.getBasicRemote().sendText("\"java没见" + count++ + "\""); // 发送文本消息
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