package br.com.alura.ecommerce;

import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var producer = new KafkaProducer<String, String>(properties());
		for (var x=0; x<100; x++) {
			var key = UUID.randomUUID().toString();
			var value = key + "21301223";
			var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
			
			Callback callback = (data, ex) -> {
				if(Objects.nonNull(ex)) {
					ex.printStackTrace();
					return;
				}
				
				System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/" + data.timestamp());
			};
			producer.send(record, callback).get();
			var email = "Welcome we are processing your order";
			var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
			producer.send(emailRecord, callback).get();
		}
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

}
