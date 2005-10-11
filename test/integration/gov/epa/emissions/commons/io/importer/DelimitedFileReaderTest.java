package gov.epa.emissions.commons.io.importer;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

public class DelimitedFileReaderTest extends TestCase {

    private DelimitedFileReader reader;

    protected void setUp() throws Exception {
        File file = new File("test/data/orl/SimpleDelimited.txt");
        reader = new DelimitedFileReader(file);
    }

    protected void tearDown() throws IOException {
        reader.close();
    }

    public void testShouldAddTrailingInlineCommentsAsASingleTokenToRecord() throws IOException {
        Record record = reader.read();

        assertNotNull(record);
        assertEquals("! EPA-derived", record.token(7));
    }

    public void testShouldReadTenRecordsOfTheMonthlyPacket() throws IOException {
        for (int i = 0; i < 10; i++) {
            Record record = reader.read();
            assertNotNull(record);
        }
    }

    public void testShouldReadFirstRecordCorrectly() throws IOException {
        Record record = reader.read();
        assertEquals(8, record.size());
        assertEquals("37119", record.token(0));
        assertEquals("0001", record.token(1));
        assertEquals("0001", record.token(2));
        assertEquals("1", record.token(3));
        assertEquals("1", record.token(4));
        assertEquals("40201301", record.token(5));
        assertEquals("02", record.token(6));
        assertEquals("! EPA-derived", record.token(7));
    }

    public void testShouldReadSecondRecordCorrectly() throws IOException {
        reader.read(); // ignore

        Record record = reader.read();
        assertEquals(7, record.size());
        assertEquals("37119", record.token(0));
        assertEquals("0001", record.token(1));
        assertEquals("0001", record.token(2));
        assertEquals("1", record.token(3));
        assertEquals("1", record.token(4));
        assertEquals("40201301", record.token(5));
        assertEquals("02", record.token(6));
    }

    public void testShouldIdentifyEndOfFile() throws IOException {
        for (int i = 0; i < 10; i++) {
            assertNotNull(reader.read());
        }

        Record end = reader.read();
        assertEquals(0, end.size());
        assertTrue("Should be the Packet Terminator", end.isEnd());

    }
}
