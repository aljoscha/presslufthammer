/**
 * 
 */
package de.tuberlin.dima.presslufthammer.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.query.parser.QueryParser.ParseError;

/**
 * @author h
 * 
 */
public class JettyClient extends AbstractHandler {
	
	private final Logger log = LoggerFactory.getLogger(getClass());

	private ParsingClient client;
	private String lastResult = "";
	private Map<Integer, String> results;

	public JettyClient(String host, int port) {
		client = new ParsingClient(host, port, this);
		client.start();
	}

	public void setLastResult(String lastResult) {
		results = Maps.newHashMap();
		Splitter splitter = com.google.common.base.Splitter.fixedLength(2048);
		Iterable<String> iter = splitter.split(lastResult);
		int i = 0;
		for (String s : iter) {
			if (s != null && s.length() > 0) {
				results.put(i++, s);
			}
		}
		// this.lastResult = temp + "</ul>";
		log.debug("Split result into {} Strings", i);
		this.lastResult = lastResult;
	}

	@Override
	public void handle(String target, HttpServletRequest request,
			HttpServletResponse response, int dispatch) throws IOException,
			ServletException {

		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		Request base_request = request instanceof Request ? (Request) request
				: HttpConnection.getCurrentConnection().getRequest();
		// Response base_response = response instanceof Response ?
		// (Response)response :
		// HttpConnection.getCurrentConnection().getResponse();

		PrintWriter writer = response.getWriter();
		if ("/result".equalsIgnoreCase(target)) {
			// String[] split = lastResult.split("\\s");
			String format = "<!DOCTYPE html><html>\n"
					+ "<head><meta charset='utf-8'></head>\n" + "<body>\n"
					+ "<h2>Result:</h2>\n" + "<div>\n";
			writer.println(format);
			// for(String s: split) {
			// format += "<li>" + s + "</li>";
			// }
			Integer index;
			try {
				index = Integer.parseInt(base_request.getParameter("page"));
			} catch(NumberFormatException e) {
				e.printStackTrace();
				index = 0;
			}
//			writer.println(results.get(index));
			writer.println(lastResult);
			format = "</div>\n" + "</body>\n" + "</html>\n";
			writer.println(format);
			// writer.format(format, new Object[] {});
		} else if (base_request.getParameterMap().containsKey("query")) {

			String query = base_request.getParameter("query");
			try {
				client.query(query);
			} catch (ParseError e) {
				e.printStackTrace();
			}
			writer.format("<!DOCTYPE html><html>\n"
					+ "<head><meta charset='utf-8'></head>\n" + "<body>\n"
					+ "<h1>Presslufthammer</h1>\n<div>Query sent.</div>\n"
					+ "<div>\n" + "<form method=\"POST\" action=\"\">\n"
					+ "<input name=\"query\" type=\"text\" />\n"
					+ "<input name=\"submit\" type=\"submit\" />\n"
					+ "</form>\n" + "</div>\n" + "</body>\n" + "</html>\n",
					new Object[] {});

		} else {

			writer.format("<!DOCTYPE html><html>\n"
					+ "<head><meta charset='utf-8'></head>\n" + "<body>\n"
					+ "<h1>Presslufthammer</h1>\n" + "<div>\n"
					+ "<form method=\"POST\" action=\"\">\n"
					+ "<input name=\"query\" type=\"text\" />\n"
					+ "<input name=\"submit\" type=\"submit\" />\n"
					+ "</form>\n" + "</div>\n" + "</body>\n" + "</html>\n",
					new Object[] {});
		}

		base_request.setHandled(true);
	}

	public static void main(String[] args) throws Exception {
		int port = 44444;
		String host = "localhost";
		if (args.length == 2) {
			port = Integer.parseInt(args[1]);
			host = args[0];
		}
		Server server = new Server(8080);
		server.setHandler(new JettyClient(host, port));

		server.start();
		server.join();
	}
}
