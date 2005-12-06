package gov.epa.emissions.commons.io.importer;


public interface Importer {
    /**
     * Imports a file into database
     */
    void run() throws Exception;

}
