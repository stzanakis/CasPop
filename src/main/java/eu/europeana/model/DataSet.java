package eu.europeana.model;

import java.util.HashSet;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class DataSet {
    String datasetId;
    String description;
    HashSet<String> schemas;

    public DataSet(String datasetId, String description, HashSet<String> schemas) {
        this.datasetId = datasetId;
        this.description = description;
        this.schemas = schemas;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public HashSet<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(HashSet<String> schemas) {
        this.schemas = schemas;
    }
}
