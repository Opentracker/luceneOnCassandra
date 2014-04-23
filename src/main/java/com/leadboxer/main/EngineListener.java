package com.leadboxer.main;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import me.prettyprint.hector.api.Keyspace;

public class EngineListener implements ServletContextListener {

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        try {
            Context envCtx = (Context) new InitialContext().lookup("java:comp/env");
            
            Keyspace keyspace = (Keyspace) envCtx.lookup("cassandra/luceneClientFactory");
            
            sce.getServletContext().setAttribute("keyspace_lucene0", keyspace);
        } catch (NamingException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
       
    }

}
