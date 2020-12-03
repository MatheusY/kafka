package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	private final KafkaDispatacher<Order> orderDispatcher = new KafkaDispatacher<>();
	private final KafkaDispatacher<String> emailDispatacher = new KafkaDispatacher<>();

	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
		emailDispatacher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			var email = req.getParameter("email");
			var orderId = UUID.randomUUID().toString();
			var amount = new BigDecimal(req.getParameter("ammount"));
			var order = new Order(orderId, amount, email);
			orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

			var emailCode = "Welcome we are processing your order";
			emailDispatacher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
			System.out.println("New order sent successfully");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("New order sent");
		} catch (InterruptedException e) {
			throw new ServletException(e);
		} catch (ExecutionException e) {
			throw new ServletException(e);
		}

	}
}