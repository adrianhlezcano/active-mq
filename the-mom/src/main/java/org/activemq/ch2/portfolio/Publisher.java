package org.activemq.ch2.portfolio;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMapMessage;

public class Publisher {
	protected int MAX_DELTA_PERCENT = 1;
	protected Map<String, Double> LAST_PRICES = new Hashtable<>();
	protected static int count = 10;
	protected static int total;
	
	protected static String brokerURL = "tcp://localhost:61616";
	protected static transient ConnectionFactory factory;
	protected static Connection connection;
	protected static Session session;
	protected static MessageProducer producer;
	
	public Publisher() throws JMSException {
		factory = new ActiveMQConnectionFactory(brokerURL);
		connection = factory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);		
	}
	
	public void close() throws JMSException {
		if (session != null)
			session.close();
		if (connection != null)
			connection.close();
	}
	
	public static void main(String [] args) throws JMSException {
		Publisher publisher = new Publisher();
		while (total < 100){
			for (int i = 0; i < count; i++){
				publisher.sendMessage(args);
			}
			total += count;
			System.out.println("Published '" + count + "' of '" + total + "' price messages");
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		publisher.close();
 	}
	
	public void sendMessage(String [] args) throws JMSException {
		int idx = 0;
		while(true){
			idx = (int) Math.round(args.length * Math.random());
			if (idx < args.length)
				break;
		}
		String stock = args[idx];
		Destination destination = session.createTopic("STOCKS." + stock);
		producer = session.createProducer(destination);
		Message message = createStockMessage(stock, session);
		System.out.println("Sending: "+ ((ActiveMQMapMessage) message).getContentMap() + " on destination: "+ destination);
		producer.send(message);
	}
	
	protected Message createStockMessage(String stock, Session session) throws JMSException {
		Double value = LAST_PRICES.get(stock);
		if (value == null){
			value = new Double(Math.random() * 100);
		}
		// lets mutate the value by some percentage
		double oldPrice = value.doubleValue();
		value = new Double(mutatePrice(oldPrice));
		LAST_PRICES.put(stock, value);
		double price = value.doubleValue();
		double offer = price * 1.001;
		boolean up = (price > oldPrice);
		
		MapMessage message = session.createMapMessage();
		message.setString("stock", stock);
		message.setDouble("price", price);
		message.setDouble("offer", offer);
		message.setBoolean("up", up);
		return message;		
	}
	
	protected double mutatePrice(double price){
		double percentageChange = (2 * Math.random() * MAX_DELTA_PERCENT) - MAX_DELTA_PERCENT;
		return price * (100 + percentageChange) / 100;
	}
}
