package gov.epa.emissions.commons.io.nif.onroad;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FixedColsTableFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImportHelper;
import gov.epa.emissions.commons.io.nif.NIFDatasetTypeUnits;

import java.io.File;

public class NIFOnRoadDatasetTypeUnits implements NIFDatasetTypeUnits {

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;

    private FormatUnit trDatasetTypeUnit;

    private NIFImportHelper delegate;

    private File[] files;

    private String tablePrefix;

    public NIFOnRoadDatasetTypeUnits(File[] files, String tablePrefix, SqlDataTypes sqlDataTypes) {
        this.files = files;
        this.tablePrefix = tablePrefix;
        FileFormat emFileFormat = new EmissionRecordsFileFormat(sqlDataTypes);
        emDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(emFileFormat, sqlDataTypes), emFileFormat,
                false);

        FileFormat peFileFormat = new EmissionPeriodsFileFormat(sqlDataTypes);
        peDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(peFileFormat, sqlDataTypes), peFileFormat,
                false);

        FileFormat trFileFormat = new TemporalRecordsFileFormat(sqlDataTypes);
        trDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(trFileFormat, sqlDataTypes), trFileFormat,
                false);
        delegate = new NIFImportHelper();
    }

    public void process() throws ImporterException {
        associateFileWithUnit(files, tablePrefix);
        requiredExist();
    }

    public FormatUnit[] formatUnits() {
        return new FormatUnit[] { emDatasetTypeUnit, peDatasetTypeUnit, trDatasetTypeUnit };
    }
    
    public String dataTable(){
        return emDatasetTypeUnit.getInternalSource().getTable();
    }

    private void associateFileWithUnit(File[] files, String tableName) throws ImporterException {
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            String key = delegate.notation(file);
            FormatUnit formatUnit = fileToDatasetTypeUnit(key);
            if (formatUnit != null) {
                formatUnit.setInternalSource(delegate.internalSource(tableName, key, file, formatUnit));
            }
        }
    }

    private void requiredExist() throws ImporterException {
        FormatUnit[] reqUnits = { emDatasetTypeUnit, peDatasetTypeUnit };
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < reqUnits.length; i++) {
            if (reqUnits[i].getInternalSource() == null) {
                sb.append("\t" + reqUnits[i].fileFormat().identify() + "\n");
            }
        }

        if (sb.length() > 0) {
            throw new ImporterException("NIF onroad import requires following types \n" + sb.toString());
        }
    }

    private FormatUnit fileToDatasetTypeUnit(String key) {
        if ("em".equals(key)) {
            return emDatasetTypeUnit;
        }

        if ("pe".equals(key)) {
            return peDatasetTypeUnit;
        }

        if ("tr".equals(key)) {
            return trDatasetTypeUnit;
        }
        return null;
    }

}
