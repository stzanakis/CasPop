package eu.europeana.tools;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.PropertiesConfigurationLayout;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Paths;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class Configuration {

    public static PropertiesConfiguration loadConfiguration(String propertiesDirectory, String propertiesFilename) throws FileNotFoundException, ConfigurationException {
        PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
        PropertiesConfigurationLayout configurationPropertiesLayout = new PropertiesConfigurationLayout(propertiesConfiguration);
        File configurationFile = Paths.get(propertiesDirectory, propertiesFilename).toFile();
        if (configurationFile.exists())
            configurationPropertiesLayout.load(new FileReader(configurationFile));
        else
            configurationPropertiesLayout.load(new FileReader(Configuration.class.getClassLoader().getResource(propertiesFilename).getFile()));

        return propertiesConfiguration;
    }
}