package com.dadfha.mobisos;

import java.io.IOException;

//import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
//import com.rabbitmq.client.GetResponse;

/**
 * Experimental communication method between Node.js and Java
 * @author Wirawit
 *
 */
public class Messenger {
	
	private final ConnectionFactory factory;
	private Connection con = null;
	private Channel channel = null;	
	
	public Messenger() {
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		
		/*
		 * Receiving message
		 * 
		boolean autoAck = false;
		GetResponse response = channel.basicGet("java2nodeQueue", autoAck);
		
		if (response == null) {
		    // No message retrieved.
		} else {
		    AMQP.BasicProperties props = response.getProps();
		    byte[] body = response.getBody();
		    long deliveryTag = response.getEnvelope().getDeliveryTag();
		    
		    
		    channel.basicAck(deliveryTag, false); // acknowledge receipt of the message
		 		
		}
		*/
		
		
	} // end of constructor
	
	public void sendMessage(final String message) {
		
		try {
			con = factory.newConnection();
			channel = con.createChannel();					
			for (int i = 0 ; i < 10 ; i++){
				channel.basicPublish("", "java2nodeQueue", false,false,null, (message + i).getBytes());    
			}				
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(channel != null) channel.close();
				if(con != null) con.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} // end try-catch			
		
	} // end of sendMessage()
	
	
	
	
}
