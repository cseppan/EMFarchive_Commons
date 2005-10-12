package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.FixedWidthPacketReader;
import gov.epa.emissions.commons.io.importer.PacketReader;
import gov.epa.emissions.commons.io.importer.Record;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class PointSourcePacketReaderTest extends MockObjectTestCase {

    private PacketReader reader;

    private BufferedReader fileReader;

    protected void setUp() throws Exception {
        File file = new File("test/data/temporal-crossreference/point-source.txt");

        Mock typeMapper = mock(SqlDataTypes.class);
        typeMapper.stubs().method(ANYTHING).will(returnValue("ANY"));

        ColumnsMetadata cols = new PointSourceColumnsMetadata((SqlDataTypes) typeMapper.proxy());
        fileReader = new BufferedReader(new FileReader(file));
        reader = new FixedWidthPacketReader(fileReader, fileReader.readLine().trim(), cols);
    }

    protected void tearDown() throws IOException {
        fileReader.close();
    }

    public void testShouldIdentifyPacketHeaderAsMonthly() {
        assertEquals("POINT DEFN", reader.identify());
    }

    public void itestShouldReadThirteenRecordsOfThePacket() throws IOException {
        for (int i = 0; i < 20; i++) {
            Record record = reader.read();
            assertNotNull(record);
        }
    }

    public void itestShouldReadFirstRecordCorrectly() throws IOException {
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

    public void itestShouldReadSecondRecordCorrectly() throws IOException {
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

    public void itestShouldIdentifyEndOfPacket() throws IOException {
        for (int i = 0; i < 13; i++) {
            assertNotNull(reader.read());
        }

        Record end = reader.read();
        assertEquals(0, end.size());
        assertTrue("Should be the Packet Terminator", end.isEnd());

    }

    public void itestShouldReadCommentsAsItReadsRecords() throws IOException {
        for (int i = 0; i < 13; i++) {
            assertNotNull(reader.read());
        }

        assertTrue("Should be the Packet Terminator", reader.read().isEnd());

        assertEquals(3, reader.comments().size());
    }
}
