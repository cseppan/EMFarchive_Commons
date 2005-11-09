package gov.epa.emissions.commons;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.configuration.ConfigurationException;

public class ConfigTest extends TestCase {

    private Config config;

    protected void setUp() throws ConfigurationException {
        config = new Config("test/integration/gov/epa/emissions/commons/config-test.conf");
    }

    public void testShouldLoadProperties() throws Exception {
        assertEquals("EMF", config.value("database.name"));
    }
    
    public void testShouldReturnProperties() throws Exception {
        Properties p = config.properties();
        assertTrue(p.size()>5);
    }
}
