package br.com.alura.ecommerce.consumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatacher;

public class KafkaService<T> implements Closeable {

	private KafkaConsumer<String, Message<T>> consumer;
	private final ConsumerFunction<T> parse;

	public KafkaService(String groupId, String topic, ConsumerFunction parse, Map<String, String> properties) {
		this(groupId, parse, properties);
		consumer.subscribe(Collections.singletonList(topic));

	}

	public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Map<String, String> properties) {
		this(groupId, parse, properties);
		consumer.subscribe(topic);
	}

	private KafkaService(String groupId, ConsumerFunction parse, Map<String, String> properties) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
	}

	public void run() throws InterruptedException, ExecutionException {
		try (var deadLetter = new KafkaDispatacher<>()) {
			while (true) {
				var records = consumer.poll(Duration.ofMillis(100));
				if (!records.isEmpty()) {
					System.out.println("Encontrei " + records.count() + " registros!");
					for (var record : records) {
						try {
							parse.consume(record);
						} catch (Exception e) {
							System.out.println("Erro");
							var message = record.value();
							deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
									message.getId().continueWith("Deadletter"),
									new GsonSerializer<>().serialize("", message));
						}
					}
				}
			}
		}
	}

	private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.putAll(overrideProperties);
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}
}
