package com.leadboxer.service;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import me.prettyprint.hector.api.Keyspace;

/**
 * Servlet implementation class EventReader
 */
public class EventReader extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private Keyspace keyspace = null;
	
	int i = 0;
	

    @Override
    public void init() throws ServletException {
        keyspace = (Keyspace) getServletContext().getAttribute("keyspace_lucene0");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	     try {
	            Keyspace ks = keyspace;
	            
	            // id = request.getRemoteAddr() + System.currentTimeMillis() + random;
	            //request.getRemoteAddr();
	            //System.currentTimeMillis();
	            //request.getHeader("User-Agent");
	            //request.getQueryString();
	            
	
	 
	            PrintWriter out = response.getWriter();
	            response.setContentType("text/html");
	            out.print("<html><body>");
	            out.print("done write " + i);
	            out.print("</body></html>");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	     i++;
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	    doGet(request,response);
	}

}
