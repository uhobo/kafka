package kafkaExamples.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer implements Runnable {
	private KafkaProducer kafkaProducer;
	
	
	public Producer(String bootstrapServers) {
		Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer(properties);
	}
	 
	protected void finalize () {
		 kafkaProducer.close();
	}
	
	public void send( String topicName, String message, Integer count){
       
        try{
            for(int i = 0; i < count; i++){
                System.out.println(i);
                kafkaProducer.send(new ProducerRecord(topicName, Integer.toString(i), message + i ));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        
    }

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
}
