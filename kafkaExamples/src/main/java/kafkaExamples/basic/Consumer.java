package kafkaExamples.basic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class Consumer implements Runnable {
	KafkaConsumer kafkaConsumer;
	
	
	public Consumer(String bootstrapServers, String groupName, List<String> topics) {
		 Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers); 
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", groupName);
        kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(topics);
	}
	
	protected void finalize () {
		kafkaConsumer.close();
	}
	
	
	
	@SuppressWarnings("rawtypes")
    public void run() {
       
		System.out.println(getMyInfo() + ": Starting consume..."  );

        try{
            while (true){
				
				ConsumerRecords records = kafkaConsumer.poll(Duration.ofSeconds(10) );
				 Iterator itr = records.iterator(); 
				while(itr.hasNext()) {
					ConsumerRecord record = (ConsumerRecord)itr.next();
					  System.out.println(String.format("%s Topic - %s, Partition - %d, Value: %s", getMyInfo(), record.topic(), record.partition(), record.value()));
				}
				
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }

	private String getMyInfo() {
		StringBuilder builder = new StringBuilder();
		return builder.append("[").append(Thread.currentThread().getName()).append("]").append(" groupMetadata:").append(kafkaConsumer.groupMetadata().toString()).toString();
	}
}