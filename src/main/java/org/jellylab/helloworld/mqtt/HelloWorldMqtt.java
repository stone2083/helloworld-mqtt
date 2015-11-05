package org.jellylab.helloworld.mqtt;

import static java.lang.String.format;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Hello World MQTT!
 *
 */
public class HelloWorldMqtt {

    public static final String BROKER = "tcp://helloworld:61613";       // helloworld ip is 112.124.41.233
    public static final String USER = "admin";                          // user name
    public static final char[] PASSWORD = "nonadmin".toCharArray();     // password
    public static final String TOPIC = "helloworld/mqtt";               // topic name

    public static void main(String[] args) throws Exception {
        // options
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setUserName(USER);
        opts.setPassword(PASSWORD);

        // consumer1, e.g. device 1
        MqttClient consumer1 = new MqttClient(BROKER, "consumer1");
        consumer1.setCallback(new Worker("consumer1"));
        consumer1.connect(opts);
        consumer1.subscribe(TOPIC);

        // consumer2, e.g. device 2
        MqttClient consumer2 = new MqttClient(BROKER, "consumer2");
        consumer2.setCallback(new Worker("consumer2"));
        consumer2.connect(opts);
        consumer2.subscribe(TOPIC);

        // consumer3, e.g. device 3
        MqttClient consumer3 = new MqttClient(BROKER, "consumer3");
        consumer3.setCallback(new Worker("consumer3"));
        consumer3.connect(opts);
        consumer3.subscribe(TOPIC);

        // publisher
        MqttClient publisher = new MqttClient(BROKER, "publisher");
        publisher.connect(opts);

        // loop for publish
        for (int i = 0; i < 10; i++) {
            String msg = format("hello world, mqtt! [id = %d]", i);
            publisher.publish(TOPIC, new MqttMessage(msg.getBytes()));
            Thread.sleep(1000);
        }

        // close for all
        publisher.disconnect();
        consumer1.disconnect();
        consumer2.disconnect();
        consumer3.disconnect();
    }

    public static class Worker implements MqttCallback {

        private String name;

        public Worker(String name) {
            this.name = name;
        }

        @Override
        public void messageArrived(String topic, MqttMessage msg) throws Exception {
            String info = format("Recived[%s-%s]: %s", name, topic, new String(msg.getPayload()));
            System.out.println(info);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }

        @Override
        public void connectionLost(Throwable e) {

        }
    }

}
