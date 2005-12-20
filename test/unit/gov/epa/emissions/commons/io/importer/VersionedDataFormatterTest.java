package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.version.VersionedRecord;
import gov.epa.emissions.commons.io.Column;

import java.util.List;

import org.jmock.MockObjectTestCase;

public class VersionedDataFormatterTest extends MockObjectTestCase {

    public void testShouldNotAppendCommentsIfItAlreadyExistsOnFlatteningData() {
        Column[] cols = { new Column("1", "int"), new Column("2", "String") };

        VersionedDataFormatter formatter = new VersionedDataFormatter(cols);

        VersionedRecord record = new VersionedRecord();
        record.add("val1");
        record.add("val2");
        record.add("!comment");

        List results = formatter.format(record, 1);

        assertEquals(4 + 3, results.size());
        assertEquals("val1", results.get(4));
        assertEquals("val2", results.get(5));
        assertEquals("!comment", results.get(6));
    }

    public void FIXME_testShouldPrefillWithDefaultsWhenNeededOnFlatteningData() {
        Column[] cols = { new Column("1", "int"), new Column("2", "String") };
        VersionedDataFormatter formatter = new VersionedDataFormatter(cols);

        VersionedRecord record = new VersionedRecord();
        record.add("val1");
        record.add("!comment");

        List results = formatter.format(record, 1);

        assertEquals(4 + 3, results.size());
        assertEquals("val1", results.get(4));
        assertEquals("", results.get(5));
        assertEquals("!comment", results.get(6));
    }

    public void FIXME_testShouldPrefillWithDefaultsWhenDataIsEmptyOnFlatteningData() {
        Column[] cols = { new Column("1", "int"), new Column("2", "String") };

        VersionedDataFormatter formatter = new VersionedDataFormatter(cols);

        VersionedRecord record = new VersionedRecord();

        List results = formatter.format(record, 1);

        assertEquals(4 + 3, results.size());
        assertEquals("", results.get(4));
        assertEquals("", results.get(5));
        assertEquals("!", results.get(6));
    }
}
