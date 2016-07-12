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
 * @since 2016-07-07
 */
public class PopulateProviderDatasetWorkflowMain {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String args[]) throws FileNotFoundException, ConfigurationException {
        System.out.println("How you doin?");
        String provider1 = "provider1";
        String dataset1 = "dataset1";
        String dataset2 = "dataset2";
        String schema1 = "schema1";

        CassandraConnector cassandraConnector = CassandraConnector.getInstance();
        Session session = cassandraConnector.getSession();
//
//        long startTime = System.currentTimeMillis();
//        //Create first provider dataset information
//        CassandraPopulator.createProviderDataset(session, provider1, dataset1, schema1);
////        CassandraPopulator.createProviderDataset(session, provider1, dataset2, schema1);
//
//        //Populate with data the dataset
//        CassandraPopulator.fillInDataset(session, provider1, dataset1, schema1, 1000, 100);
//
//        long stopTime = System.currentTimeMillis();
//        long elapsedTime = stopTime - startTime;
//        logger.info("Populate provider dataset workflow in total time: " + elapsedTime + "ms");

        CassandraTruncator.mcsTruncate(session);

        cassandraConnector.closeSession();
        cassandraConnector.closeConnection();

    }
}
