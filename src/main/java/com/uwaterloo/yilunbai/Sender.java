package com.uwaterloo.yilunbai;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.UUID;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

// mvn exec:java -Dexec.mainClass=com.uwaterloo.yilunbai.Sender -Dexec.args="host username password datasize"
public class Sender implements AutoCloseable {
    final private static int MAX_BYTES = 134217728;
    
    private Connection connection;
    private Channel channel;
    private static long totalTime;

    public Sender(String host, String username, String password) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
		factory.setUsername(username);
		factory.setPassword(password);

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] args) {
        String host = args[0];
		String username = args[1];
		String password = args[2];
		long datasize = Integer.parseInt(args[3]);

        long current = 0;
        byte[] request;

        try (Sender sender = new Sender(host, username, password)) {
            while (current != datasize) {
                if (datasize - current >= MAX_BYTES) {
                    request = new byte[MAX_BYTES];
                    current += MAX_BYTES;
                } else {
                    request = new byte[(int) (datasize - current)];
                    current = datasize;
                }
                String response = sender.run(request);
                System.out.println("Consumer received bytes: " + response);
            }

            double throughput = (double) datasize * (1000000000L) / totalTime;
            System.out.println("Throughput: " + getFormatSize(throughput)+"/s");
            
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String run(byte[] request) throws IOException, InterruptedException {
        String corrId = UUID.randomUUID().toString();

        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName)
                .build();

        long startTime = System.nanoTime();
        channel.basicPublish("", "q1", props, request);

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody()));
            }
        }, consumerTag -> {
        });

        String responseStr = response.take();
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        totalTime += duration;
        System.out.println("Request/Response took: " + duration + " ns");

        channel.basicCancel(ctag);
        return responseStr;
    }

    public void close() throws IOException {
        connection.close();
    }

    private static DecimalFormat df = null;
 
    static {
        // set format
        df = new DecimalFormat("#0.0");
        df.setRoundingMode(RoundingMode.HALF_UP);
        df.setMaximumFractionDigits(1);
        df.setMinimumFractionDigits(1);
    }

    //format 
    static private String getFormatSize(double length) {
        double size = length / (1 << 30);
        if(size >= 1) {
            return df.format(size) + "GB";
        }
        size = length / (1 << 20);
        if(size >= 1) {
            return df.format(size) + "MB";
        }
        size = length / (1 << 10);
        if(size >= 1) {
            return df.format(size) + "KB";
        }
        return df.format(length) + "B";
    }
}
