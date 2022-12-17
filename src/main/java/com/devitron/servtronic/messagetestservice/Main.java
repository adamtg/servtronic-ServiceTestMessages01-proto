package com.devitron.servtronic.messagetestservice;

import com.devitron.servtronic.messages.ServiceRegistration;
import com.devitron.servtronic.messagetestservice.messages.MessageSetTest01;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import javax.naming.Binding;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Main {


    private static String registrationExchange = "MessageController";
    private static String registrationRoutingKey = "registration";

    private static String mcIncomingExchange = "MessageController";
    private static String mcIncomingRoutingKey = "incoming";

    public static String serviceName = "testsender01";
    public static String exchangeServer = "exchangetester";
    public static String routingKey = "testsender01";

    public static void register(Channel channel) {
        ServiceRegistration.Request request = new ServiceRegistration.Request(
                serviceName, routingKey, exchangeServer);



        ObjectMapper objectMapper = new ObjectMapper();
        String stringMessage = null;
        try {
            stringMessage = objectMapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        System.out.println("--------------------------------------");
        System.out.println(stringMessage);
        System.out.println("--------------------------------------");

        try {
            channel.basicPublish(registrationExchange, registrationRoutingKey, null, stringMessage.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection  = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeServer, "direct", true);
        channel.queueDeclare(serviceName,true, false, false, null);
        channel.queueBind(serviceName, exchangeServer, routingKey);


        register(channel);

        ObjectMapper objectMapper = new ObjectMapper();
        MessageSetTest01.PrintHelloRequest request = new MessageSetTest01.PrintHelloRequest(serviceName);
        request.setFieldInteger(24);
        request.setFieldString("Best Number");
        String stringMessage = objectMapper.writeValueAsString(request);
        System.out.println("========================================");
        System.out.println(stringMessage);
        System.out.println("========================================");

        channel.basicPublish(mcIncomingExchange, mcIncomingRoutingKey, null, stringMessage.getBytes());

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        channel.basicConsume(serviceName, true, deliverCallback, consumerTag -> { });
    }

}
