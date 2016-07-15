package eu.europeana;

import com.datastax.driver.core.Session;
import eu.europeana.tools.CassandraConnector;
import eu.europeana.tools.CassandraTruncator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-12
 */
public class MCSTruncatorMain {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String args[]) throws Exception {
        logger.info("MCS Truncator");
        String provider = "provider2";
        String dataset = "dataset2";
        String schema = "schema1";

        int batch = 50;
        int fetchSize = 1000;
        int rowsThreshold = 500;
        try(CassandraConnector cassandraConnector = CassandraConnector.getInstance(); Session session = cassandraConnector.getSession()) {
//            CassandraTruncator.assignmentsRepresentationsTruncate(session);

            CassandraTruncator.cleanAssignmentsRepresentationsFromProvider(session, provider, dataset, schema, fetchSize, rowsThreshold, batch);
        }
    }
}
