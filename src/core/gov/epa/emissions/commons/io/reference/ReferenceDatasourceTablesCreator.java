package gov.epa.emissions.commons.io.reference;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ReferenceDatasourceTablesCreator {

    private Datasource reference;

    private SqlDataTypes sqlDataTypes;

    private File folder;

    public ReferenceDatasourceTablesCreator(DbServer dbServer, File folder) {
        this.reference = dbServer.getReferenceDatasource();
        this.sqlDataTypes = dbServer.getSqlDataTypes();
        this.folder = folder;
    }

    public void create() throws ImporterException {
        Importer importer = null;

        File pollutantFile = new File(folder, "pollutants.txt");
        importer = new ReferenceCSVFileImporter(pollutantFile, "pollutants", reference, sqlDataTypes);
        importer.run();
        System.out.println("Pollutants table created.");

        File countiesFile = new File(folder, "counties.txt");
        importer = new ReferenceCSVFileImporter(countiesFile, "fips", reference, sqlDataTypes);
        importer.run();
        System.out.println("Counties table created.");

        File countriesFile = new File(folder, "countries.txt");
        importer = new ReferenceCSVFileImporter(countriesFile, "countries", reference, sqlDataTypes);
        importer.run();
        System.out.println("Countries table created.");

        File sccFile = new File(folder, "scc.txt");
        importer = new ReferenceCSVFileImporter(sccFile, "scc", reference, sqlDataTypes);
        importer.run();
        System.out.println("SCC table created.");

        File sectorsFile = new File(folder, "sectors.txt");
        importer = new ReferenceCSVFileImporter(sectorsFile, "sectors", reference, sqlDataTypes);
        importer.run();
        System.out.println("Sectors table created.");

        File statesFile = new File(folder, "states.txt");
        importer = new ReferenceCSVFileImporter(statesFile, "states", reference, sqlDataTypes);
        importer.run();
        System.out.println("States table created.");

        System.out.println("Reference Datasource setup completed.");
    }

}
