package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try (var orderDispatcher = new KafkaDispatacher<Order>()) {
			try (var emailDispatacher = new KafkaDispatacher<String>()) {
				for (var x = 0; x < 10; x++) {
					var userId = UUID.randomUUID().toString();
					var orderId = UUID.randomUUID().toString();
					var amount = new BigDecimal(Math.random() * 5000 + 1);
					var order = new Order(userId, orderId, amount);

					orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

					var email = "Welcome we are processing your order";
					emailDispatacher.send("ECOMMERCE_SEND_EMAIL", userId, email);
				}
			}

		}
	}

}
