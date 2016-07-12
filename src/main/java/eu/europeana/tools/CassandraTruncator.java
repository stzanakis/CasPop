package eu.europeana.tools;

import com.datastax.driver.core.Session;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-12
 */
public class CassandraTruncator {
    private static final Logger logger = LogManager.getLogger();
    public static void assignmentsRepresentationsTruncate(Session session)
    {
        logger.info("Database starting truncation!");

        long startTime = System.currentTimeMillis();
        String query = "TRUNCATE ecloud_mcs.data_set_assignments_cloud_id; ";
        session.execute(query);
        query = "TRUNCATE ecloud_mcs.data_set_assignments_provider_dataset_revision; ";
        session.execute(query);
        query = "TRUNCATE ecloud_mcs.data_set_assignments_provider_dataset_schema; ";
        session.execute(query);
        query = "TRUNCATE ecloud_mcs.representation_revisions; ";
        session.execute(query);
        query = "TRUNCATE ecloud_mcs.representation_revisions_timestamp; ";
        session.execute(query);

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Database truncated in " + elapsedTime + "ms");
    }

    public static void cleanAssignmentsRepresentationsFromProvider(String provider)
    {
        logger.info("Database cleanup from provider: " + provider);

        long startTime = System.currentTimeMillis();




        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Database cleaned in " + elapsedTime + "ms");
    }
}
