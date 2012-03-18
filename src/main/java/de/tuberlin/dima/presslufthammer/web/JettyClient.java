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

	private static final String CSS = "<style type=\"text/css\">\n"
			+ "#main { margin:2em 3em 0 3em; color:#4959aa; font-size:150%;}\n"
			+ "h1 { border-bottom: 1px dashed grey; margin:0 1em 1em 1em;}\n"
			+ "#qfield { width:50%; height:2em; border:thin solid #4959aa;}\n"
			+ "#qsub { height:2em; border:none; background:#4959aa; color:white; font-weight:bold;}\n"
			+ "#qform { text-align:center; margin: 0 0 2em 0;}\n"
			+ "#info { text-align:center; border-top: 1px dashed grey; margin: 0 1em 0 1em;}\n"
			+ "#result { border-top: 1px dashed grey; margin: 1em 1em 1em 1em;"
			+ " font-size:75%;}\n" + "</style>\n";
	private static final String HEADER = "<!DOCTYPE html><html>\n"
			+ "<head><meta charset='utf-8'>\n"
			+ "<title>Presslufthammer</title>\n"
			+ "<script src=\"http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js\">"
			+ "</script>\n" + CSS + "</head>\n" + "<body>\n";
	private static final String FORM = "<form id=\"qform\" action=\"#\">\n"
			+ "<input id=\"qsub\" name=\"submit\" type=\"submit\" value=\"Query\" />\n"
			+ "<input id=\"qfield\" name=\"query\" type=\"text\" />\n"
			+ "</form>\n" + "<div id=\"output\"></div>\n";
	private static final String FOOTER = "<script type=\"text/javascript\">\n"
			+ "$('#qform').submit(function(event) {\n"
			+ "event.preventDefault();\n"
			+ "var q = $('#qfield').val();"
			+ "$.post('', { query: q}, function(data) {\n"
			+ "$('#output').empty().append(data)}).complete(setTimeout(loadResult, 2000));\n"//
			+ "});\n" + "function loadResult() {\n"
			+ "$.get('/result',function(data) { $('#output').append(data)});"
			+ "}\n" + "</script>\n</body>\n</html>\n";

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

		String content = "";
		PrintWriter writer = response.getWriter();
		if ("/result".equalsIgnoreCase(target)) {
			// String[] split = lastResult.split("\\s");
			content = "<div id=\"result\"><h2>Result:</h2>\n" + "<div>\n";
			// writer.println(format);
			// for(String s: split) {
			// format += "<li>" + s + "</li>";
			// }
			// Integer index;
			// try {
			// index = Integer.parseInt(base_request.getParameter("page"));
			// } catch (NumberFormatException e) {
			// e.printStackTrace();
			// index = 0;
			// }
			// writer.println(results.get(index));
			content += ((lastResult != null) ? lastResult
					: "No data available.") + "</div>\n</div>\n";
			// writer.format(format, new Object[] {});
		} else if (base_request.getParameterMap().containsKey("query")) {

			String query = base_request.getParameter("query");
			String info = "<div id=\"info\" style=\"color:green;\">Query sent</div>\n";
			try {
				client.query(query);
			} catch (ParseError e) {
				info = "<div id=\"info\" style=\"color:red;\"><b>ParseError:</b> "
						+ e.getMessage() + "</div>\n";
				e.printStackTrace();
			}

			content = info;
		} else {
			content = HEADER
					+ "<div id=\"main\"><h1 style=\"align:center;\">Presslufthammer</h1>\n"
					+ FORM + "</div>" + FOOTER;
		}
		writer.println(content);

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
