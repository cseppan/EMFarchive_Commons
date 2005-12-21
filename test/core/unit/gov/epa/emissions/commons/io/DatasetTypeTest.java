package gov.epa.emissions.commons.io;

import junit.framework.TestCase;

public class DatasetTypeTest extends TestCase {
    
    public void testAddKeyword() {
        DatasetType type = new DatasetType();
        Keyword kw = new Keyword();
        kw.setName("key1");
        type.addKeyword(kw);

       Keyword[] actual = type.getKeywords();
        assertEquals(1, actual.length);
        assertEquals("key1", actual[0].getName());
    }

    public void testSetKeywords() {
        DatasetType type = new DatasetType();
        Keyword kw1 = new Keyword("key1");
        Keyword kw2 = new Keyword("key2");
        
        type.setKeywords(new Keyword[]{kw1, kw2});
        
        Keyword[] actual = type.getKeywords();
        assertEquals(2, actual.length);
        assertEquals("key1", actual[0].getName());
        assertEquals("key2", actual[1].getName());
    }
}
