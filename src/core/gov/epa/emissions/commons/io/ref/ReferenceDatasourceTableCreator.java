package gov.epa.emissions.commons.io.ref;

import java.io.File;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.reference.ReferenceCVSFileImporter;

public class ReferenceDatasourceTableCreator {
    
    private Datasource reference;
    
    private SqlDataTypes sqlDataTypes;

    public ReferenceDatasourceTableCreator(DbServer dbServer){
        this.reference = dbServer.getReferenceDatasource();
        this.sqlDataTypes = dbServer.getSqlDataTypes();
    }
    
    public void create() throws ImporterException{
        Importer importer = null;
        String refDir = "config/ref";
        
        File pollutantFile = new File(refDir,"pollutants.txt");
        importer = new ReferenceCVSFileImporter(pollutantFile,"pollutants",reference,sqlDataTypes);
        importer.run();
        
        File countiesFile = new File(refDir,"counties.txt");
        importer = new ReferenceCVSFileImporter(countiesFile,"fips",reference,sqlDataTypes);
        importer.run();
        
        File countriesFile = new File(refDir,"countries.txt");
        importer = new ReferenceCVSFileImporter(countriesFile,"countries",reference,sqlDataTypes);
        importer.run();
        
        File sccFile = new File(refDir,"scc.txt");
        importer = new ReferenceCVSFileImporter(sccFile,"scc",reference,sqlDataTypes);
        importer.run();
        
        File sectorsFile = new File(refDir,"sectors.txt");
        importer = new ReferenceCVSFileImporter(sectorsFile,"sectors",reference,sqlDataTypes);
        importer.run();
        
        File statesFile = new File(refDir,"states.txt");
        importer = new ReferenceCVSFileImporter(statesFile,"states",reference,sqlDataTypes);
        importer.run();
        
    }

}
