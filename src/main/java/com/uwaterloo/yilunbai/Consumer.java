package com.uwaterloo.yilunbai;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.*;

// mvn exec:java -Dexec.mainClass=com.uwaterloo.yilunbai.Consumer -Dexec.args="host username password"
public class Consumer {
    public static void main(String[] args) throws IOException, TimeoutException {
        String host = args[0];
		String username = args[1];
        String password = args[2];
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
		factory.setUsername(username);
		factory.setPassword(password);

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare("q1", false, false, false, null);
            channel.queuePurge("q1");

            System.out.println("Awaiting requests");

            Object monitor = new Object();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                        .correlationId(delivery.getProperties().getCorrelationId()).build();

                String response = "" + delivery.getBody().length;
                System.out.println("Receive data bytes: " + delivery.getBody().length);

                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            };

            channel.basicConsume("q1", false, deliverCallback, (consumerTag -> {
            }));

            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
}
