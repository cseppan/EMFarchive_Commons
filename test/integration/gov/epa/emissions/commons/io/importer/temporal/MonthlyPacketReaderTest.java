package gov.epa.emissions.commons.io.importer.temporal;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

public class MonthlyPacketReaderTest extends TestCase {

    private PacketReader reader;

    protected void setUp() throws Exception {
        File file = new File("test/data/temporal-profiles/monthly.txt");
        reader = new PacketReader(file);
    }

    protected void tearDown() throws IOException {
        reader.close();
    }

    public void testShouldIdentifyPacketHeaderAsMonthly() throws IOException {
        assertEquals("MONTHLY", reader.identify());
    }

    public void testShouldReadTenRecordsOfTheMonthlyPacket() throws IOException {
        reader.identify();

        for (int i = 0; i < 10; i++) {
            Record record = reader.read();
            assertNotNull(record);
        }
    }

    public void testShouldReadFirstRecordCorrectly() throws IOException {
        reader.identify();

        Record record = reader.read();

        assertEquals(14, record.size());

        assertEquals("    1", record.token(0));
        assertEquals("   0", record.token(1));
        assertEquals("   0", record.token(2));
        assertEquals("   0", record.token(3));
        assertEquals("   0", record.token(4));
        assertEquals("   0", record.token(5));
        assertEquals(" 110", record.token(6));
        assertEquals(" 110", record.token(7));
        assertEquals(" 110", record.token(8));
        assertEquals(" 223", record.token(9));
        assertEquals(" 223", record.token(10));
        assertEquals(" 223", record.token(11));
        assertEquals("   0", record.token(12));
        assertEquals("  999", record.token(13));
    }
    
    public void testShouldReadSecondRecordCorrectly() throws IOException {
        reader.identify();
        reader.read(); //ignore
        
        Record record = reader.read();
        
        assertEquals(14, record.size());
        
        assertEquals("    2", record.token(0));
        assertEquals("   0", record.token(1));
        assertEquals("   0", record.token(2));
        assertEquals("   0", record.token(3));
        assertEquals("   0", record.token(4));
        assertEquals("   0", record.token(5));
        assertEquals(" 290", record.token(6));
        assertEquals(" 290", record.token(7));
        assertEquals(" 290", record.token(8));
        assertEquals("  43", record.token(9));
        assertEquals("  43", record.token(10));
        assertEquals("  43", record.token(11));
        assertEquals("   0", record.token(12));
        assertEquals("  999", record.token(13));
    }
}
