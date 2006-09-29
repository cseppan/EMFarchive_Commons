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
        importRefFile("SCCCodes.csv", "SCC_Codes");
        importRefFile("ControlDevice.csv", "Control_Device");
        importRefFile("DataSourceCodes.csv", "DataSource_Codes");
        importRefFile("EmissionReleasePointType.csv", "Emission_Release_Point_Type");
        importRefFile("LocationDefaultCodes.csv", "Location_Default_Codes");
        importRefFile("MactCodes.csv", "Mact_Codes");
        importRefFile("MACTComplianceStatus.csv", "MACT_Compliance_Status");
        importRefFile("PollutantCodes.csv", "Pollutant_codes");
        importRefFile("TribalCodes.csv", "tribal_codes");
        importRefFile("StateCountyFIPsCodes.csv", "State_County_FIPs_Codes");
        importRefFile("StackDefaultCodes.csv", "Stack_Default_Codes");
        importRefFile("SICtoMACTDefaults.csv", "SIC_To_MACT_Defaults");
        importRefFile("SICCodes.csv", "SIC_Codes");
        importRefFile("NAICStoMACTDefaults.csv", "NAICS_to_MACT_Defaults");
        importRefFile("NAICSCodes.csv", "NAICS_Codes");
        importRefFile("MultipleMACTCodes.csv", "Multiple_MACT_Codes");

        importRefFile("pollutants.txt", "pollutants");
        importRefFile("counties.txt", "fips");
        importRefFile("countries.txt", "countries");
        importRefFile("scc.txt", "scc");
        importRefFile("sectors.txt", "sectors");
        importRefFile("states.txt", "states");
        importRefFile("gdplev.txt", "gdplev");

        System.out.println("Reference Datasource setup completed.");
    }

    private void importRefFile(String fileName, String tableName) throws ImporterException {
        File pollutantFile = new File(folder, fileName);
        Importer importer = new ReferenceCSVFileImporter(pollutantFile, tableName, reference, sqlDataTypes);
        importer.run();
        System.out.println(tableName + " table created.");
    }

}
