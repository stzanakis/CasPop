package eu.europeana;

import com.datastax.driver.core.Session;
import eu.europeana.tools.CassandraConnector;
import eu.europeana.tools.CassandraPopulator;
import eu.europeana.tools.CassandraTruncator;
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

    public static void main(String args[]) throws FileNotFoundException, ConfigurationException, InterruptedException {
        logger.info("Copy workflow");
        String providerFrom = "provider1";
        String providerTo = "provider2";
        String datasetFrom = "dataset1";
        String datasetTo = "dataset2";
        String schema1 = "schema1";

        CassandraConnector cassandraConnector = CassandraConnector.getInstance();
        Session session = cassandraConnector.getSession();

        CassandraPopulator.createProviderDataset(session, providerTo, datasetTo, schema1);

        int runTimes = 1;
        long totalRunsElapsedTime = 0;
        int batch = 50;
        int sleepTime = 5000;
        int fetchSize = 1000;
        int rowsThreshold = 500;
        int limit = 1000;
        for(int i = 0; i < runTimes; i++) {
            long startTime = System.currentTimeMillis();
            CopyWorkflow.copyFromProviderDatasetPublished(session, providerFrom, providerTo, datasetFrom, datasetTo, schema1, fetchSize, rowsThreshold, limit, batch);
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Run: " + i + ", Copy providerFrom: " + providerFrom + " datasetFrom: " + datasetFrom + " to providerTo: " + providerTo
                    + " datasetTo: " + datasetTo + " in total time: " + elapsedTime + "ms");

            CassandraTruncator.cleanAssignmentsRepresentationsFromProvider(session, providerTo, datasetTo, schema1, fetchSize, rowsThreshold, batch);
            totalRunsElapsedTime += elapsedTime;
            logger.info("Sleep for: " + sleepTime + "ms");
            Thread.sleep(sleepTime);
        }
        logger.info("Average speed of " + runTimes + " run times is: " + totalRunsElapsedTime/runTimes + "ms");

        cassandraConnector.closeSession();
        cassandraConnector.closeConnection();
    }
}
