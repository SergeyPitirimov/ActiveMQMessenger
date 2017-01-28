/* This is the Apache ActiveMQ messenger */
/* Before starting the program, please, be sure that activemq service is already running */
/* by typing this: http://localhost:8161/admin in your Internet browser */
/* Author: Sergey Pitirimov. Feel free to contact me by email: sergey.pitirimov@gmail.com */
package messenger;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Messenger implements Runnable
{
  public void run()
  {
    Connection readConnection = null;

    try
    {
      ActiveMQConnectionFactory jmsConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      readConnection = jmsConnectionFactory.createConnection();
      readConnection.start();
      Session readSession = readConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue readMessageQueue = readSession.createQueue("Messages");
      MessageConsumer consumer = readSession.createConsumer(readMessageQueue);
      
      /* Listen for messages */
      for(int i = 0; i < 3; i++)
      {
        TextMessage readMessage = (TextMessage)consumer.receive();
        String readText = readMessage.getText();
        System.out.println("Received message: " + readText);
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

    if (readConnection != null)
    {
      try
      {
        readConnection.close();
      }
      catch(JMSException e)
      {
        e.printStackTrace();
      }
    }
    System.out.println("Consumer's connection has been successfully closed.");
  }

  public static void main(String[] args)
  {
    Connection writeConnection = null;

    /* Create new object to read messages and run it in the new thread */
    Runnable messenger = new Messenger();
    Thread t = new Thread(messenger);
    t.start();

    /* Create the message writer */
    try
    {
      ActiveMQConnectionFactory jmsConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      writeConnection = jmsConnectionFactory.createConnection();
      writeConnection.start();
      Session writeSession = writeConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue writeMessageQueue = writeSession.createQueue("Messages");
      MessageProducer producer = writeSession.createProducer(writeMessageQueue);
      
      /* Send the messages */
      for(int i = 0; i < 3; i++)
      {
        TextMessage writeMessage = writeSession.createTextMessage("This is the message " + i);
        producer.send(writeMessage);
        System.out.println("Sent message: " + writeMessage.toString());
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

    if (writeConnection != null)
    {
      try
      {
        writeConnection.close();
      }
      catch(JMSException e)
      {
        e.printStackTrace();
      }
    }

    System.out.println("Producer's connection has been successfully closed.");
  }
}