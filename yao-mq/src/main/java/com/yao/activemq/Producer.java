package com.yao.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.*;

/**
 * @author lirui
 * @date 2018-07-11
 * @description MQ消息生产者
 */
public class Producer {

    public static void sendMsg(String text){
        Connection connection = null;
        try {
            //连接工厂
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://10.12.5.69:61616");
            //获取连接
            connection = connectionFactory.createConnection();
            //必须开启连接,否则无法发送消息
            connection.start();
            //创建会话
            //第一个参数表示是否开启事务
            //第二参数消息确认默认,AUTO_ACKNOWLEDGE:自动确认,消费端receive即可
            //CLIENT_ACKNOWLEDGE客户端确认，需要客户端手动调用确认方法
            Session session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);

            //创建目的地即发送的queue或者topic
            Queue queue1 = session.createQueue("queue1");
            //创建消息生产者
            MessageProducer producer = session.createProducer(queue1);
            //创建消息内容有5种类型，这里创建TestMessage
            TextMessage textMessage = new ActiveMQTextMessage();
            textMessage.setText(text);

            //发送消息
            producer.send(textMessage);
            //消息默认持久化的
            //DeliveryMode.PERSISTENT ： 持久化，activemq重启，消息还在
            //DeliveryMode.NON_PERSISTENT : 非持久化 activemq重启 消息丢失
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            //注意：创建session时第一个参数为false所以不开启事务,消息发送成功
            //但是如果开启事务，这时是不会发送消息的，session必须commit消息才能发送成功，这里是可以rollback的

            //当开始事务时必须commit才能发送成功
            //session.commit();

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if(connection != null){
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        sendMsg("hello world");
    }

}
