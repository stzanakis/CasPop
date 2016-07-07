package eu.europeana.model;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public enum RevisionVocabulary {
    TRANSFORM("TRANSFORM"),
    DEREFERENCE("DEREFERENCE"),
    ENRICH("ENRICH"),
    ACCEPTANCE("ACCEPTANCE"),
    PUBLISH("PUBLISH");

    private String revisionPrefix;

    RevisionVocabulary(String revisionPrefix) {
        this.revisionPrefix = revisionPrefix;
    }
}
