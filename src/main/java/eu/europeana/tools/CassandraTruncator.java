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

        String query = "SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION + " WHERE "
                + McsConstansts.PROVIDER_ID + "='" + provider + "' AND " + McsConstansts.DATASET_ID + "='" + dataset + "'" + " AND " + McsConstansts.SCHEMA_ID + "='" + schema + "'";

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

        session.execute("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION + " WHERE "
                + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?", provider, dataset, schema);
        session.execute("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_SCHEMA + " WHERE "
                + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?", provider, dataset, schema);

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Database cleaned in " + elapsedTime + "ms");
    }

    private static void deleteCloudIds(Session session, String provider, String dataset, String schema, List<String> cloudIds, int batch) {
        int totalRecords = cloudIds.size();
        for (int j = 1; j <= totalRecords; j += batch) {
            long startTime = System.currentTimeMillis();
            PreparedStatement ps1 = session.prepare("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_CLOUD_ID + " WHERE "
                    + McsConstansts.CLOUD_ID + "=?" + " AND " + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?");
            PreparedStatement ps2 = session.prepare("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS + " WHERE "
                    + McsConstansts.CLOUD_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?");
            PreparedStatement ps3 = session.prepare("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS_TIMESTAMP + " WHERE "
                    + McsConstansts.CLOUD_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?");
            BatchStatement batch1 = new BatchStatement();
            BatchStatement batch2 = new BatchStatement();
            BatchStatement batch3 = new BatchStatement();
            for (int i = j; i < j + batch; i++) {
                String cloudId = cloudIds.get(i-1);
                batch1.add(ps1.bind(cloudId, provider, dataset, schema));
                batch2.add(ps2.bind(cloudId, schema));
                batch3.add(ps3.bind(cloudId, schema));
            }
            session.execute(batch1);
            session.execute(batch2);
            session.execute(batch3);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Deleted batch: " + batch + " assignments and representations for provider: " + provider + " and dataset: " + dataset + " in: " + elapsedTime + "ms. Until now populated: " + (batch + j-1));
        }
    }
}
