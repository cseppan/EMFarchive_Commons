package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.temporal.PointTemporalReferenceReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class PointTemporalReferenceReaderTest extends MockObjectTestCase {

    private PointTemporalReferenceReader reader;

    private BufferedReader fileReader;

    protected void setUp() throws Exception {
        File file = new File("test/data/temporal-crossreference/point-source.txt");

        Mock typeMapper = mock(SqlDataTypes.class);
        typeMapper.stubs().method(ANYTHING).will(returnValue("ANY"));

        fileReader = new BufferedReader(new FileReader(file));
        reader = new PointTemporalReferenceReader(fileReader);
    }

    protected void tearDown() throws IOException {
        fileReader.close();
    }

    public void testShouldIdentifyPacketHeader() {
        assertEquals("POINT DEFN", reader.identify());
    }

    public void testShouldReadTwentyRecordsOfThePacket() throws IOException {
        for (int i = 0; i < 20; i++) {
            Record record = reader.read();
            assertNotNull(record);
        }
    }

    public void testShouldReadFirstRecordCorrectly() throws IOException {
        Record record = reader.read();

        assertEquals(5, record.size());

        assertEquals("0000000000", record.token(0));
        assertEquals("262", record.token(1));
        assertEquals("7", record.token(2));
        assertEquals("24", record.token(3));
        assertEquals("-9", record.token(4));
    }

    public void testShouldReadSecondRecordCorrectly() throws IOException {
        reader.read(); // ignore

        Record record = reader.read();

        assertEquals(5, record.size());

        assertEquals("10100101", record.token(0));
        assertEquals("462", record.token(1));
        assertEquals("8", record.token(2));
        assertEquals("33", record.token(3));
        assertEquals("-9", record.token(4));
    }

    public void testShouldIdentifyEndOfPacket() throws IOException {
        for (int i = 0; i < 20; i++) {
            assertNotNull(reader.read());
        }

        Record end = reader.read();
        assertEquals(0, end.size());
        assertTrue("Should be the Packet Terminator", end.isEnd());

    }

    public void testShouldReadCommentsAsItReadsRecords() throws IOException {
        for (int i = 0; i < 20; i++) {
            assertNotNull(reader.read());
        }

        assertTrue("Should be the Packet Terminator", reader.read().isEnd());

        assertEquals(3, reader.comments().size());
    }
}
