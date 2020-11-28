package br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try (var dispatcher = new KafkaDispatacher()) {
			for (var x=0; x<10; x++) {
				var key = UUID.randomUUID().toString();
				var value = key + "21301223";
				
				dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
				
				var email = "Welcome we are processing your order";
				dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
			}
			
		}
	}

}
