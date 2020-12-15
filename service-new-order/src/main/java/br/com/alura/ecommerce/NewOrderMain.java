package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import br.com.alura.ecommerce.dispatcher.KafkaDispatacher;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try (var orderDispatcher = new KafkaDispatacher<Order>()) {
			var email = Math.random() + "@email.com";
			for (var x = 0; x < 10; x++) {
				var orderId = UUID.randomUUID().toString();
				var amount = new BigDecimal(Math.random() * 5000 + 1);
				var order = new Order(orderId, amount, email);

				var id = new CorrelationId(NewOrderMain.class.getSimpleName());

				orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
			}

		}
	}

}
