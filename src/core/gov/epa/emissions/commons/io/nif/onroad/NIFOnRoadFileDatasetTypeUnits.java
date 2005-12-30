package gov.epa.emissions.commons.io.nif.onroad;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class NIFOnRoadFileDatasetTypeUnits extends NIFOnRoadDatasetTypeUnits {

    private File[] files;

    private String tablePrefix;

    public NIFOnRoadFileDatasetTypeUnits(File[] files, String tablePrefix, SqlDataTypes sqlDataTypes) {
        super(sqlDataTypes);
        this.files = files;
        this.tablePrefix = tablePrefix;
    }

    public void process() throws ImporterException {
        associateFileWithUnit(files, tablePrefix);
        requiredExist();
    }

    private void associateFileWithUnit(File[] files, String tableName) throws ImporterException {
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            String key = delegate.notation(file);
            FormatUnit formatUnit = keyToDatasetTypeUnit(key);
            if (formatUnit != null) {
                formatUnit.setInternalSource(delegate.internalSource(tableName, key, file, formatUnit));
            }
        }
    }
}
