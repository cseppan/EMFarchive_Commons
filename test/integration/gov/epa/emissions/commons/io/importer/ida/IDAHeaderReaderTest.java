package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.io.importer.DbTestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class IDAHeaderReaderTest extends DbTestCase {

    public void testShouldIdentifyPollutants() throws Exception {
        File file = new File("test/data/ida/small-area.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        IDAHeaderReader headerReader = new IDAHeaderReader(reader);
        headerReader.read();

        // assert
        String [] polls = headerReader.polluntants();
        assertEquals(polls[0],"VOC");
        assertEquals(polls[1],"NOX");
        assertEquals(polls[2],"CO");
        assertEquals(polls[3],"SO2");
        assertEquals(polls[4],"PM10");
        assertEquals(polls[5],"PM2_5");
        assertEquals(polls[6],"NH3");
        assertEquals(10,headerReader.comments().size());
    }



}

