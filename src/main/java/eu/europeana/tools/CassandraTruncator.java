package eu.europeana.tools;

import com.datastax.driver.core.*;
import eu.europeana.model.McsConstansts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        String query = "TRUNCATE " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_CLOUD_ID;
        session.execute(query);
        query = "TRUNCATE " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION;
        session.execute(query);
        query = "TRUNCATE " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_SCHEMA;
        session.execute(query);
        query = "TRUNCATE " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS;
        session.execute(query);
        query = "TRUNCATE " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS_TIMESTAMP;
        session.execute(query);

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Database truncated in " + elapsedTime + "ms");
    }

    public static void cleanAssignmentsRepresentationsFromProvider(Session session, String provider, String dataset, String schema, int fetchSize, int rowsThreshold, int batch)
    {
        logger.info("Database cleanup from provider: " + provider);

        long startTime = System.currentTimeMillis();

        String query = "SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS + " WHERE "
                + McsConstansts.PROVIDER_ID + "=" + provider;

        List<String> cloudIds = new ArrayList<String>(fetchSize);
        Statement stmt = new SimpleStatement(query);
        stmt.setFetchSize(fetchSize);
        ResultSet rs = session.execute(stmt);
        Iterator<Row> iterator = rs.iterator();
        int counter = 0;
        while (iterator.hasNext()) {
            if (rs.getAvailableWithoutFetching() == rowsThreshold && !rs.isFullyFetched())
                rs.fetchMoreResults();

            Row row = iterator.next();
            if(row.getString("provider_id").equals(provider)) {
                cloudIds.add(row.getString("cloud_id"));
                counter++;
            }

            if(counter%fetchSize == 0) {
                //Delete assignments from everywhere
                deleteCloudIds(session, provider, dataset, schema, cloudIds, batch);
                logger.info("Total processed until now: " + counter);
                cloudIds = new ArrayList<String>(fetchSize);
            }
        }


        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Database cleaned in " + elapsedTime + "ms");
    }

    private static void deleteCloudIds(Session session, String provider, String dataset, String schema, List<String> cloudIds, int batch) {
        int totalRecords = cloudIds.size();
        for (int j = 1; j <= totalRecords; j += batch) {
            long startTime = System.currentTimeMillis();
            StringBuilder stringBuilder1 = new StringBuilder("BEGIN BATCH ");
            StringBuilder stringBuilder2 = new StringBuilder("BEGIN BATCH ");
            StringBuilder stringBuilder3 = new StringBuilder("BEGIN BATCH ");
            StringBuilder stringBuilder4 = new StringBuilder("BEGIN BATCH ");
            StringBuilder stringBuilder5 = new StringBuilder("BEGIN BATCH ");
            for (int i = j; i < j + batch; i++) {
                String cloudId = cloudIds.get(i-1);
                stringBuilder1.append("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_CLOUD_ID + " WHERE "
                        + McsConstansts.CLOUD_ID + "=" + cloudId);
                stringBuilder2.append("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION + " WHERE "
                        + McsConstansts.PROVIDER_ID + "=" + provider + " AND " + McsConstansts.DATASET_ID + "=" + dataset + " AND " + McsConstansts.CLOUD_ID + "=" + cloudId);
                stringBuilder3.append("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_SCHEMA + " WHERE "
                        + McsConstansts.PROVIDER_ID + "=" + provider + " AND " + McsConstansts.DATASET_ID + "=" + dataset + " AND " + McsConstansts.CLOUD_ID + "=" + cloudId);
                stringBuilder4.append("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS + " WHERE "
                        + McsConstansts.CLOUD_ID + "=" + cloudId);
                stringBuilder5.append("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS_TIMESTAMP + " WHERE "
                        + McsConstansts.CLOUD_ID + "=" + cloudId + " AND " + McsConstansts.SCHEMA_ID + "=" + schema);
            }
            stringBuilder1.append(" APPLY BATCH;");
            stringBuilder2.append(" APPLY BATCH;");
            stringBuilder3.append(" APPLY BATCH;");
            stringBuilder4.append(" APPLY BATCH;");
            stringBuilder5.append(" APPLY BATCH;");
            session.execute(stringBuilder1.toString());
            session.execute(stringBuilder2.toString());
            session.execute(stringBuilder3.toString());
            session.execute(stringBuilder4.toString());
            session.execute(stringBuilder5.toString());

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Deleted batch: " + batch + " assignments and representations for provider: " + provider + " and dataset: " + dataset + " in: " + elapsedTime + "ms. Until now populated: " + (batch + j-1));
        }
    }
}
