package gov.epa.emissions.commons.io.importer;

public interface NewImporter {

    /**
     *(Create table or tables for inventory dataset)
     * Read files and insert into the tables
     * load information into the dataset
     */
    void run() throws Exception;

}
