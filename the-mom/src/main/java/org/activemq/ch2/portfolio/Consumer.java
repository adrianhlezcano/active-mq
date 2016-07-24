package org.activemq.ch2.portfolio;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer {
	private static String brokerURL = "tcp://localhost:61616";
	private static transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Session session;
	
	public Consumer() throws JMSException {
		factory = new ActiveMQConnectionFactory(brokerURL);
		connection = factory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}
	
	public void close() throws JMSException{
		if (session != null )
			session.close();
		if (connection != null)
			connection.close();
	}
	
	public Session getSession() {
		return session;
	}
	
	public static void main(String[] args) throws JMSException{
		if (args.length == 0)
			throw new IllegalArgumentException("Args should be provided");
		
		Consumer consumer = new Consumer();
		for (String stock : args){
			Destination destination = consumer.getSession().createTopic("STOCKS."+ stock);
			MessageConsumer messageConsumer = consumer.getSession().createConsumer(destination);
			messageConsumer.setMessageListener(new Listener());			
		}
	}

}
