package gov.epa.emissions.commons.io.nif.onroad;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImportHelper;

public class NIFOnRoadTableDatasetTypeUnits extends NIFOnRoadDatasetTypeUnits {

    private String[] tables;

    private Datasource datasource;

    public NIFOnRoadTableDatasetTypeUnits(String[] tables, DbServer dbServer, SqlDataTypes sqlDataTypes, 
            DataFormatFactory factory) {
        super(sqlDataTypes, factory);
        this.tables = tables;
        this.datasource = dbServer.getEmissionsDatasource();
    }

    public void process() throws ImporterException {
        associateTables(tables, datasource);
        requiredExist();
    }

    private void associateTables(String[] tables, Datasource datasource) {
        NIFImportHelper helper = new NIFImportHelper();
        for (int i = 0; i < tables.length; i++) {
            String key = helper.notation(datasource, tables[i]);
            FormatUnit formatUnit = keyToDatasetTypeUnit(key);
            if (formatUnit != null) {
                formatUnit.setInternalSource(helper.internalSource(tables[i], tables[i], formatUnit));
            }
        }
    }
}
