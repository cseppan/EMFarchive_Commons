package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.PacketReader;
import gov.epa.emissions.commons.io.importer.Record;

import java.io.File;
import java.io.IOException;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class WeeklyPacketReaderTest extends MockObjectTestCase {

    private PacketReader reader;

    protected void setUp() throws Exception {
        File file = new File("test/data/temporal-profiles/weekly.txt");

        Mock typeMapper = mock(SqlDataTypes.class);
        typeMapper.stubs().method("getInt").will(returnValue("int"));
        typeMapper.stubs().method("getLong").will(returnValue("long"));

        ColumnsMetadata cols = new WeeklyColumnsMetadata((SqlDataTypes) typeMapper.proxy());
        reader = new PacketReader(file, cols);
    }

    public void testShouldIdentifyPacketHeaderAsMonthly() {
        assertEquals("WEEKLY", reader.identify());
    }

    public void testShouldReadThirteenRecordsOfTheWeeklyPacket() throws IOException {
        for (int i = 0; i < 13; i++) {
            Record record = reader.read();
            assertNotNull(record);
        }
    }

    public void testShouldReadFirstRecordCorrectly() throws IOException {
        Record record = reader.read();

        assertEquals(9, record.size());

        assertEquals("    1", record.token(0));
        assertEquals(" 200", record.token(1));
        assertEquals(" 200", record.token(2));
        assertEquals(" 200", record.token(3));
        assertEquals(" 200", record.token(4));
        assertEquals(" 200", record.token(5));
        assertEquals("   0", record.token(6));
        assertEquals("   0", record.token(7));
        assertEquals(" 1000", record.token(8));
    }

    public void testShouldReadSecondRecordCorrectly() throws IOException {
        reader.read(); // ignore

        Record record = reader.read();

        assertEquals(9, record.size());

        assertEquals("    2", record.token(0));
        assertEquals(" 200", record.token(1));
        assertEquals(" 200", record.token(2));
        assertEquals(" 200", record.token(3));
        assertEquals(" 200", record.token(4));
        assertEquals(" 200", record.token(5));
        assertEquals("   0", record.token(6));
        assertEquals("   0", record.token(7));
        assertEquals(" 1000", record.token(8));
    }

    public void testShouldReturnAllRecords() throws IOException {
        assertEquals(13, reader.allRecords().size());
    }

    public void testShouldIdentifyEndOfPacket() throws IOException {
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();
        reader.read();

        Record end = reader.read();
        assertEquals(0, end.size());
        assertTrue("Should be the Packet Terminator", end.isEnd());

    }
}
