package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.importer.DelimitedFileReader;
import gov.epa.emissions.commons.io.importer.Record;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

public class ORLReaderTest extends TestCase {

    private DelimitedFileReader reader;

    private String dataFolder = "test/data/orl/nc";

    protected void tearDown() throws IOException {
        reader.close();
    }

    public void testShouldIdentifyFirstSixLinesOfSmallPointFileAsComments() throws Exception {
        File file = new File(dataFolder, "small-point.txt");
        reader = new DelimitedFileReader(file);

        assertNotNull(reader.read());
        assertEquals(6, reader.comments().size());
    }

    public void testShouldCollectAllTenCommentsOfSmallPointFile() throws Exception {
        File file = new File(dataFolder, "small-point.txt");
        reader = new DelimitedFileReader(file);

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

    public void testShouldCreateRecordWithTwelveTokensForEachLineOfSmallNonPointFile() throws Exception {
        File file = new File(dataFolder, "small-nonpoint.txt");
        reader = new DelimitedFileReader(file);

        Record record = reader.read();
        assertNotNull(record);
        assertEquals(12, record.size());
    }
    
    public void FIXME_testVariationsOfDelimiterWidthsAndQuotesInAPointFile() throws Exception {
        File file = new File(dataFolder, "point-with-variations.txt");
        reader = new DelimitedFileReader(file);
        
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());

        assertTrue(reader.read().isEnd());
    }
    
    public void testVariationsOfDelimiterWidthsAndQuotesInTheSmallPointFile() throws Exception {
        File file = new File(dataFolder, "small-point.txt");
        reader = new DelimitedFileReader(file);
        
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        assertEquals(28, reader.read().size());
        
        assertTrue(reader.read().isEnd());
    }
}
