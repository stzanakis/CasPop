package eu.europeana;

import com.datastax.driver.core.Session;
import eu.europeana.tools.CassandraConnector;
import eu.europeana.tools.CassandraPopulator;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class Main {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String args[]) throws FileNotFoundException, ConfigurationException {
        System.out.println("How you doin?");

        CassandraConnector cassandraConnector = CassandraConnector.getInstance();
        Session session = cassandraConnector.getSession();

        long startTime = System.currentTimeMillis();
        CassandraPopulator.populateFirstProvider(session, 1000000, 20);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated first provider in total time: " + elapsedTime + "ms");

//        ResultSet execute = session.execute("SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS);
//        List<Row> all = execute.all();
//        for (Row row :
//                all) {
//            System.out.println(row);
//        }

        cassandraConnector.closeSession();
        cassandraConnector.closeConnection();

    }
}
