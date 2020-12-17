package br.com.alura.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatacher;

public class EmailNewOrderService implements ConsumerService<Order>{

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner(EmailNewOrderService::new).start(1);

	}

	private final KafkaDispatacher<String> emailDispatacher = new KafkaDispatacher<>();

	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
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

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}

}
