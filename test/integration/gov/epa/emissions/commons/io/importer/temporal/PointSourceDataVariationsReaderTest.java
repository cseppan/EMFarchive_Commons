package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.PointSourceReader;
import gov.epa.emissions.commons.io.importer.Record;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class PointSourceDataVariationsReaderTest extends MockObjectTestCase {

    private PointSourceReader reader;

    private BufferedReader fileReader;

    protected void setUp() throws Exception {
        File file = new File("test/data/temporal-crossreference/point-source-VARIABLE-COLS.txt");

        Mock typeMapper = mock(SqlDataTypes.class);
        typeMapper.stubs().method(ANYTHING).will(returnValue("ANY"));

        ColumnsMetadata cols = new PointSourceColumnsMetadata((SqlDataTypes) typeMapper.proxy());
        fileReader = new BufferedReader(new FileReader(file));
        reader = new PointSourceReader(fileReader, cols);
    }

    protected void tearDown() throws IOException {
        fileReader.close();
    }

    public void testShouldReadTwentyRecordsOfThePacket() throws IOException {
        for (int i = 0; i < 20; i++) {
            Record record = reader.read();
            assertNotNull(record);
        }
    }

    public void testShouldReadFirstRecordCorrectly() throws IOException {
        Record record = reader.read();

        assertEquals(12, record.size());

        assertEquals("0000000000", record.token(0));
        assertEquals("262", record.token(1));
        assertEquals("7", record.token(2));
        assertEquals("24", record.token(3));
        assertEquals("1", record.token(4));
        assertEquals("2", record.token(5));
        assertEquals("3", record.token(6));
        assertEquals("4", record.token(7));
        assertEquals("5", record.token(8));
        assertEquals("6", record.token(9));
        assertEquals("7", record.token(10));
        assertEquals("8", record.token(11));
    }
    
    public void testShouldReadSixTokensIntoSecondRecord() throws IOException {
        assertNotNull(reader.read());
        Record record = reader.read();
        
        assertEquals(6, record.size());
        
        assertEquals("10100101", record.token(0));
        assertEquals("462", record.token(1));
        assertEquals("8", record.token(2));
        assertEquals("33", record.token(3));
        assertEquals("-9", record.token(4));
        assertEquals("1", record.token(5));
    }
}
