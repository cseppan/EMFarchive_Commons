package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;

/**
 * This class contains all the common features for importing formatted files.
 * FIXME: split the larger methods, and reorganize
 */
public abstract class FormattedImporter implements Importer {
    /** Dataset in which imported data is stored */
    protected Dataset dataset = null;

    protected boolean useTransactions = false;

    protected DbServer dbServer;

    protected FormattedImporter(DbServer dbServer) {
        this.dbServer = dbServer;
    }

    /**
     * Override this method in order to perform an necessary post processing,
     * meaning any processing to ocur after all of the valid data tuples are
     * inserted into the table. This is table type specific processing performed
     * after each table is imported, as opposed to dataset type specific
     * processing in postImport() which is performed after ALL tables are
     * imported. Default behavior is to not perform any post processing.
     * 
     * @param tableType2
     */
    protected void postProcess(Datasource datasource, String table, String tableType) throws Exception {
        return;/* DO NOTHING */
    }

    protected abstract String[] breakUpLine(String line, int[] widths) throws Exception;

    public final Dataset getDataset() {
        return dataset;
    }

    protected void checkForIndexOutOfBounds(int index, String[] stringlets, String line) throws Exception {
        // check for index out of bounds
        if (index >= stringlets.length) {
            throw new Exception("There are more tokens than expected for the following line:\n" + line);
        }
    }
}
