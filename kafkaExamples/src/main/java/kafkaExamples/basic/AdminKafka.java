package kafkaExamples.basic;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;


public class AdminKafka implements Runnable
{
	AdminClient client;
	private Properties properties;
	
	public AdminKafka(String bootstrapServers) {
		properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        client = AdminClient.create(properties);
	}

	protected void finalize () {
		client.close();
	}
	
    public Boolean isTopicExist(String topicName) throws InterruptedException, ExecutionException {
    	return client.listTopics().namesToListings().get().containsKey(topicName);
    }
	
	
	
    public void createTopics( String topicName, Integer numPartitions) {
        
    	
        CreateTopicsResult result = client.createTopics(Arrays.asList(
                new NewTopic(topicName, numPartitions, (short) 1)
        ));
        try {
            result.all().get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
        System.out.println("Topic " + topicName + " was created");
       
    }
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
}