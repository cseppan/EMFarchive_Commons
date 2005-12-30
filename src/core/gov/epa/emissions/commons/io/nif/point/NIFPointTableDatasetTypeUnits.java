package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImportHelper;

public class NIFPointTableDatasetTypeUnits extends NIFPointDatasetTypeUnits {

    private String[] tables;

    private Datasource datasource;

    public NIFPointTableDatasetTypeUnits(String[] tables, Datasource datasource, SqlDataTypes sqlDataTypes) {
        super(sqlDataTypes);
        this.tables = tables;
        this.datasource = datasource;
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
