package org.activemq.ch2.jobs;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.SessionInfo;

public class Producer {
	private static String brokerURL = "tcp://localhost:61616";
	private static transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Session session;
	private transient MessageProducer producer;
	
	private static int count = 10;
	private static int total;
	private static int id = 1_000_000;
	
	private String jobs[] = new String[]{"delete", "suspend"};
	
	public Producer() throws JMSException {
		factory = new ActiveMQConnectionFactory(brokerURL);
		connection = factory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		producer = session.createProducer(null);
	}
	
	public void close() throws JMSException {
		if (connection != null)
			connection.close();
	}
	
	public static void main(String[] arg) throws JMSException {
		Producer producer = new Producer();
		while (total < 1000) {
			for (int i = 0; i < count; i++){
				producer.sendMessage();
			}
			total += count;
			System.out.println("Sent '" + count + "' of '"+ total + "' job message");
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		producer.close();
	}
	
	public void sendMessage() throws JMSException {
		int idx = 0;
		
		while(true){
			idx = (int) Math.round(jobs.length * Math.random());
			if (idx < jobs.length) 
				break;
		}
		String job = jobs[idx];
		Destination destination = session.createQueue("JOBS." + job);
		Message message = session.createObjectMessage(id++);
		System.out.println("Sending: id: "+  ((ObjectMessage) message).getObject() + " on queue: "+ destination);
		producer.send(destination, message);
	}

}
