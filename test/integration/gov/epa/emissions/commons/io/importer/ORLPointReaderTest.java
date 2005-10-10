package gov.epa.emissions.commons.io.importer;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

public class ORLPointReaderTest extends TestCase {

    private DelimitedFileReader reader;

    protected void setUp() throws Exception {
        File file = new File("test/data/orl/nc/small-point.txt");
        reader = new DelimitedFileReader(file);
    }

    protected void tearDown() throws IOException {
        reader.close();
    }

    public void testShouldIdentifyFirstSixLinesAsComments() throws Exception {
        assertNotNull(reader.read());
        assertEquals(6, reader.comments().size());
    }
    
    public void testShouldCollectAllTenComments() throws Exception {
        reader.read();
        assertEquals(6, reader.comments().size());

        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        assertEquals(7, reader.comments().size());
        
        reader.read();
        assertTrue(reader.read().isEnd());
        assertEquals(8, reader.comments().size());
    }

}
