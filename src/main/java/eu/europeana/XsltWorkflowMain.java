package eu.europeana;

import com.datastax.driver.core.Session;
import eu.europeana.model.RevisionVocabulary;
import eu.europeana.tools.CassandraConnector;
import eu.europeana.tools.XsltWorkflow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-14
 */
public class XsltWorkflowMain {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args) throws Exception {
        logger.info("XSLT workflow");
        String provider = "provider2";
        String dataset = "dataset2";
        String schema = "schema1";
        String revision = RevisionVocabulary.COPY.toString() + "-1";
        String newSchema = "schemaXSLT";

        int runTimes = 10;
        long totalRunsElapsedTime = 0;
        long[] firstThree = new long[3];
        int batch = 50;
        int sleepTime = 5000;
        int fetchSize = 1000;
        int rowsThreshold = 500;
        try(CassandraConnector cassandraConnector = CassandraConnector.getInstance(); Session session = cassandraConnector.getSession()) {
            for (int i = 0; i < runTimes; i++) {
                long startTime = System.currentTimeMillis();
                XsltWorkflow.transformRecordsFromRevision(session, provider, dataset, schema, revision, newSchema, fetchSize, rowsThreshold, batch);
                long stopTime = System.currentTimeMillis();
                long elapsedTime = stopTime - startTime;
                totalRunsElapsedTime += elapsedTime;
                logger.info("Run: " + (i + 1) + ", XSLT transformation provider: " + provider + " dataset: " + dataset + " schema: " + schema
                        + " to newSchema: " + newSchema + " in total time: " + elapsedTime + "ms");

                if (i < 3)
                    firstThree[i] = elapsedTime;
//                CassandraTruncator.cleanAssignmentsRepresentationsFromProvider(session, provider, dataset, newSchema, fetchSize, rowsThreshold, batch);
                logger.info("Sleep for: " + sleepTime + "ms");
                Thread.sleep(sleepTime);
            }
//        CassandraTruncator.cleanAssignmentsRepresentationsFromProvider(session, provider, dataset, newSchema, fetchSize, rowsThreshold, batch);
        }
        logger.info("First three runs: {}ms, {}ms, {}ms", firstThree[0], firstThree[1], firstThree[2]);
        logger.info("Average speed of " + runTimes + " run times is: " + totalRunsElapsedTime/runTimes + "ms");
    }
}
