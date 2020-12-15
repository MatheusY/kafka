package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatacher;

public class EmailNewOrderService {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var emailService = new EmailNewOrderService();
		try (var service = new KafkaService(EmailNewOrderService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				emailService::parse, Map.of())) {
			service.run();
		}

	}

	private final KafkaDispatacher<String> emailDispatacher = new KafkaDispatacher<>();

	private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
		System.out.println("-----------------------------------");
		System.out.println("Processing new order, preparing email");
		System.out.println(record.value());
		var message = record.value();
		var order = message.getPayload();
		var correlationId = message.getId();

		var emailCode = "Welcome we are processing your order";
		emailDispatacher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), 
				correlationId.continueWith(EmailNewOrderService.class.getSimpleName()), emailCode);
	}

}
