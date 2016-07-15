package eu.europeana.tools;

import com.datastax.driver.core.*;
import eu.europeana.model.McsConstansts;
import eu.europeana.model.RevisionVocabulary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-15
 */
public class PublishWorkflow {
    private static final Logger logger = LogManager.getLogger();
    public static void publishDataset(Session session,String provider, String dataset, String schema, String revision, int fetchSize, int rowsThreshold, int batch)
    {
        List<String> cloudIds = new ArrayList<String>(fetchSize);
        Statement stmt = new SimpleStatement("SELECT cloud_id FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION
                + " WHERE " + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?"
                + " AND " + McsConstansts.REVISION_ID + "=?", provider, dataset, schema, revision);
        stmt.setFetchSize(fetchSize);
        ResultSet rs = session.execute(stmt);
        Iterator<Row> iterator = rs.iterator();
        int counter = 0;
        while (iterator.hasNext()) {
            if (rs.getAvailableWithoutFetching() == rowsThreshold && !rs.isFullyFetched())
                rs.fetchMoreResults();

            Row row = iterator.next();
            cloudIds.add(row.getString("cloud_id"));
            counter++;

            if(counter%fetchSize == 0) {
                ResultSet resultSet = retrieveRepresentations(session, cloudIds, schema, revision);
                createNewRevisionPublished(session, provider, dataset, resultSet, batch);
                logger.info("Total processed until now: " + counter);
                cloudIds = new ArrayList<String>(fetchSize);
            }
        }
        if(counter%fetchSize != 0) {
            ResultSet resultSet = retrieveRepresentations(session, cloudIds, schema, revision);
            createNewRevisionPublished(session, provider, dataset, resultSet, batch);
        }
        logger.info("Total processed: " + counter);
    }

    public static ResultSet retrieveRepresentations(Session session, List<String> cloudIds, String schema, String revision)
    {
        ArrayList<ResultSet> resultSets = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder("SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS
                + " WHERE " + McsConstansts.CLOUD_ID + " IN (");
        for (int i =0; i < cloudIds.size(); i++) {
            String cloudId = cloudIds.get(i);
            if(i == (cloudIds.size()-1))
                stringBuilder.append("'" + cloudId + "'");
            else
                stringBuilder.append("'" + cloudId + "',");
        }
        stringBuilder.append(") AND " + McsConstansts.SCHEMA_ID + "='" + schema + "' AND " + McsConstansts.REVISION_ID + "='" + revision + "'");
        ResultSet resultSet = session.execute(stringBuilder.toString());

        return resultSet;
    }

    public static void createNewRevisionPublished(Session session, String provider, String dataset, ResultSet rows, int batch)
    {
        Iterator<Row> iterator = rows.iterator();
        List<String> cloudIds = new ArrayList<>();
        List<String> versionIds = new ArrayList<>();
        String cloudId;
        String schemaId = null;
        String versionId;
        while (iterator.hasNext()) {
            Row row = iterator.next();
            cloudId = row.getString("cloud_id");
            schemaId = row.getString("schema_id");
            versionId = row.getString("version_id");
            cloudIds.add(cloudId);
            versionIds.add(versionId);
        }

        long startTime = System.currentTimeMillis();
        CassandraPopulator.populateRepresentations(session, provider, schemaId, cloudIds, versionIds, RevisionVocabulary.PUBLISH.toString(), batch);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated total:" + cloudIds.size() + " representations for provider " + provider + " in: " + elapsedTime + "ms");

        startTime = System.currentTimeMillis();
        CassandraPopulator.populateAssignments(session, provider, dataset, schemaId, cloudIds, RevisionVocabulary.PUBLISH.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total: " + cloudIds.size() + " assignments for provider: " + provider + " and dataset: " + dataset + " in: " + elapsedTime + "ms");
}
}
