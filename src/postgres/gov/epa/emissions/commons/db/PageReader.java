package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.db.version.ScrollableVersionedRecords;
import gov.epa.emissions.commons.db.version.VersionedRecord;

import java.sql.SQLException;

public class PageReader {
    private int pageSize;

    private ScrollableVersionedRecords scrollableRecords;

    public PageReader(int pageSize, ScrollableVersionedRecords scrollableRecords) throws SQLException {
        this.pageSize = pageSize;
        this.scrollableRecords = scrollableRecords;
        scrollableRecords.execute();
    }

    public int totalRecords() throws SQLException {
        return scrollableRecords.total();
    }

    public int totalPages() throws SQLException {
        return identifyPage(scrollableRecords.total());
    }

    /**
     * @param record
     *            starts at index '1' through n (total records)
     */
    public Page pageByRecord(int record) throws SQLException {
        return page(identifyPage(record));
    }

    private int identifyPage(int record) {
        float val = (float) record / pageSize;
        return (int) Math.ceil(val);
    }

    /**
     * 
     * @param pageNumber
     *            starts at index '1' through n (total pages)
     */
    public Page page(int pageNumber) throws SQLException {
        int actualPage = pageNumber - 1; // page '1' maps to page '0'
        if (actualPage > totalPages())
            return null;

        int start = actualPage * pageSize;
        int end = start + pageSize - 1;// since, end is inclusive in the range
        VersionedRecord[] records = scrollableRecords.range(start, end);

        Page page = new Page(pageNumber);
        page.setRecords(records);

        return page;
    }

    public void close() throws SQLException {
        scrollableRecords.close();
    }
}