package gov.epa.emissions.commons.io;

import junit.framework.TestCase;

public class DatasetTypeTest extends TestCase {

    public void testAddKeyword() {
        DatasetType type = new DatasetType();
        type.addKeyword("key1");

        String[] actual = type.getKeywords();
        assertEquals(1, actual.length);
        assertEquals("key1", actual[0]);
    }

    public void testSetKeywords() {
        DatasetType type = new DatasetType();
        type.setKeywords(new String[]{"key1", "key2"});
        
        String[] actual = type.getKeywords();
        assertEquals(2, actual.length);
        assertEquals("key1", actual[0]);
        assertEquals("key2", actual[1]);
    }
}
