package eu.europeana;

import com.datastax.driver.core.Session;
import eu.europeana.tools.CassandraConnector;
import eu.europeana.tools.CopyWorkflow;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-11
 */
public class CopyWorkflowMain {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String args[]) throws FileNotFoundException, ConfigurationException {
        System.out.println("Copy workflow");

        CassandraConnector cassandraConnector = CassandraConnector.getInstance();
        Session session = cassandraConnector.getSession();

        long startTime = System.currentTimeMillis();
        CopyWorkflow.copyFromProviderDatasetPublished(session, "provider1", "providerFirst", "dataset1", "datasetFirst", "Schema1", 1000, 500, 0);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Copy provider dataset in total time: " + elapsedTime + "ms");


        cassandraConnector.closeSession();
        cassandraConnector.closeConnection();
    }
}
