package eu.europeana.model;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public enum DefaultPrefixes {
    PROVIDER("provider"),
    DATASET("dataset");

    private String prefix;

    public String getPrefix() {
        return prefix;
    }

    DefaultPrefixes(String prefix) {
        this.prefix = prefix;
    }
}
