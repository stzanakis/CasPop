package eu.europeana.model;

import java.util.HashSet;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class Provider {
    String providerId;
    HashSet<DataSet> datasets;

    public Provider(String providerId, HashSet<DataSet> datasets) {
        this.providerId = providerId;
        this.datasets = datasets;
    }

    public Provider(String providerId) {
        this.providerId = providerId;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public HashSet<DataSet> getDatasets() {
        return datasets;
    }

    public void setDatasets(HashSet<DataSet> datasets) {
        this.datasets = datasets;
    }
}
