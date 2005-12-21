package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.db.version.VersionedRecord;
import junit.framework.TestCase;

public class PageTest extends TestCase {

    public void testShouldAddRecord() {
        Page page = new Page();

        page.add(new VersionedRecord());
        page.add(new VersionedRecord());

        assertEquals(2, page.count());
    }

    public void testShouldReturnRangeRepresentingMinAndMaxRecordIds() {
        Page page = new Page();

        assertEquals(-1, page.min());
        assertEquals(-1, page.max());

        VersionedRecord record1 = new VersionedRecord();
        record1.setRecordId(1);
        page.add(record1);
        
        VersionedRecord record2 = new VersionedRecord();
        record2.setRecordId(2);
        page.add(record2);
        
        VersionedRecord record3 = new VersionedRecord();
        record3.setRecordId(3);
        page.add(record3);
        
        assertEquals(1, page.min());
        assertEquals(3, page.max());
    }

    public void testShouldGetRecords() {
        Page page = new Page();

        VersionedRecord record1 = new VersionedRecord();
        page.add(record1);
        VersionedRecord record2 = new VersionedRecord();
        page.add(record2);

        VersionedRecord[] records = page.getRecords();

        assertEquals(2, records.length);
        assertEquals(record1, records[0]);
        assertEquals(record2, records[1]);
    }

    public void testShouldSetRecords() {
        Page page = new Page();

        VersionedRecord record1 = new VersionedRecord();
        VersionedRecord record2 = new VersionedRecord();
        page.setRecords(new VersionedRecord[] { record1, record2 });

        VersionedRecord[] records = page.getRecords();

        assertEquals(2, records.length);
        assertEquals(record1, records[0]);
        assertEquals(record2, records[1]);
    }

    public void testShouldRemoveRecord() {
        Page page = new Page();

        page.add(new VersionedRecord());
        page.add(new VersionedRecord());

        assertTrue("Should be able to remove record 1", page.remove(1));
        assertFalse("Should be unable to remove record 12", page.remove(7));

        assertEquals(1, page.count());
    }
}
