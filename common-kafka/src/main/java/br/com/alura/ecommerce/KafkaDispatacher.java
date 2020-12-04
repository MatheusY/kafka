package br.com.alura.ecommerce;

import java.io.Closeable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaDispatacher<T> implements Closeable{

	private KafkaProducer<String, T> producer;

	public KafkaDispatacher() {
		producer = new KafkaProducer<>(properties());
	}
	
	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		return properties;
	}

	void send(String topic, String key, T value) throws InterruptedException, ExecutionException {
		var record = new ProducerRecord<>(topic, key, value);
		
		Callback callback = (data, ex) -> {
			if(Objects.nonNull(ex)) {
				ex.printStackTrace();
				return;
			}
			
			System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/" + data.timestamp());
		};
		producer.send(record, callback).get();
		
	}

	@Override
	public void close() {
		producer.close();
	}
}
