package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GenerateAllReportsServlet extends HttpServlet {

private static final long serialVersionUID = 1L;
	
	private final KafkaDispatacher<String> batchDispatcher = new KafkaDispatacher<>();

	@Override
	public void destroy() {
		super.destroy();
		batchDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			batchDispatcher.send("SEND_MESSAGE_TO_ALL_USERS", "USER_GENERATE_READING_REPORT", "USER_GENERATE_READING_REPORT");
			
			System.out.println("Sent generate report to all users");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("Report request generated!");
		} catch (InterruptedException e) {
			throw new ServletException(e);
		} catch (ExecutionException e) {
			throw new ServletException(e);
		}

	}
}