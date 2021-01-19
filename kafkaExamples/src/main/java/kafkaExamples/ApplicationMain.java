package kafkaExamples;

import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafkaExamples.basic.AdminKafka;
import kafkaExamples.basic.Consumer;
import kafkaExamples.basic.Producer;

public class ApplicationMain {
	static final String bootstrapServers = "localhost:9092";
	static AdminKafka adminKafka =  new AdminKafka(bootstrapServers);
	static Producer producer = new Producer(bootstrapServers);
	static ExecutorService executor = Executors.newWorkStealingPool();
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Scanner scanner = new Scanner(System.in); 
		int choice = 0;
		do {
			System.out.println("Hello to Kafka examples!!!");
			System.out.println("1. create a Topic");
			System.out.println("2. send a message");
			System.out.println("3. Run consumer");
			System.out.println("4. exit");
			choice = scanner.nextInt();
			
			switch(choice) {
				case 1:createTopic(); break;
				case 2:createProducer();break;
				case 3: createConsumer();break;
				default: continue;
			}
			
		}while(choice != 4);
		
			
			
			
			
		
		try {
		    System.out.println("attempt to shutdown executor");
		    executor.shutdown();
		    executor.awaitTermination(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
		    System.err.println("tasks interrupted");
		}
		finally {
		    if (!executor.isTerminated()) {
		        System.err.println("cancel non-finished tasks");
		    }
		    executor.shutdownNow();
		    System.out.println("shutdown finished");
		}
	}
	
	public static void createProducer() {
		Scanner scanner = new Scanner(System.in); 
		System.out.println("Type Topic Name[test1]:");
		String topicName = scanner.nextLine();
		if(topicName == null || topicName.isEmpty()) {
			topicName = "test1";
		}
		System.out.println("Type text data:");
		String message = scanner.nextLine();
		producer.send(topicName, message, 1);
	}
	
	public static void createConsumer() {
		Scanner scanner = new Scanner(System.in); 
		System.out.println("Type Topic Name[test1]:");
		String topicName = scanner.nextLine();
		if(topicName == null || topicName.isEmpty()) {
			topicName = "test1";
		}
		System.out.println("Type group id:");
		String groupId = scanner.nextLine();
		executor.execute(new Consumer(bootstrapServers, groupId, Arrays.asList(topicName)));
	}
	
	
	public static void createTopic() {
		Scanner scanner = new Scanner(System.in); 
		System.out.println("Type Topic Name:");
		String topicName = scanner.nextLine();
		System.out.println("Type Topic number of Partitions [1]:");
		int numPartitions = scanner.nextInt();
		if(numPartitions < 1) {
			numPartitions = 1;
		}
		adminKafka.createTopics(topicName, numPartitions);
	}

}
