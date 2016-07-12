package eu.europeana;

import com.datastax.driver.core.Session;
import eu.europeana.tools.CassandraConnector;
import eu.europeana.tools.CassandraTruncator;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-12
 */
public class MCSTruncatorMain {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String args[]) throws FileNotFoundException, ConfigurationException {
        logger.info("MCS Truncator");
        CassandraConnector cassandraConnector = CassandraConnector.getInstance();
        Session session = cassandraConnector.getSession();

        CassandraTruncator.mcsTruncate(session);

        cassandraConnector.closeSession();
        cassandraConnector.closeConnection();

    }
}
