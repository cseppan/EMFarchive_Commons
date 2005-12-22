package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFDatasetTypeUnitDelegate;
import gov.epa.emissions.commons.io.nif.NIFDatasetTypeUnits;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.File;

public class NIFNonRoadDatasetTypeUnits implements NIFDatasetTypeUnits {

    private FormatUnit ceDatasetTypeUnit;

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit epDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;

    private NIFDatasetTypeUnitDelegate delegate;

    public NIFNonRoadDatasetTypeUnits(SqlDataTypes sqlDataTypes) {
        FileFormat ceFileFormat = new ControlEfficiencyFileFormat(sqlDataTypes, "NIF3.0 Nonroad Control Efficiency");
        ceDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(ceFileFormat, sqlDataTypes), ceFileFormat,
                false);

        FileFormat emFileFormat = new EmissionRecordsFileFormat(sqlDataTypes, "NIF3.0 Nonroad Emission Records");
        emDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(emFileFormat, sqlDataTypes), emFileFormat,
                false);

        FileFormat epFileFormat = new EmissionProcessFileFormat(sqlDataTypes, "NIF3.0 Nonroad Emission Process");
        epDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(epFileFormat, sqlDataTypes), epFileFormat,
                false);

        FileFormat peFileFormat = new EmissionPeriodsFileFormat(sqlDataTypes, "NIF3.0 Nonroad Emission Periods");
        peDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(peFileFormat, sqlDataTypes), peFileFormat,
                false);

        delegate = new NIFDatasetTypeUnitDelegate();
    }

    public void process(File[] files, String tableName) throws ImporterException {
        associateFileWithUnit(files, tableName);
        requiredExist();
    }

    public FormatUnit[] formatUnits() {
        return new FormatUnit[] { ceDatasetTypeUnit, emDatasetTypeUnit, epDatasetTypeUnit, peDatasetTypeUnit };
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
        FormatUnit[] reqUnits = { emDatasetTypeUnit, epDatasetTypeUnit };
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < reqUnits.length; i++) {
            if (reqUnits[i].getInternalSource() == null) {
                sb.append("\t" + reqUnits[i].fileFormat().identify() + "\n");
            }
        }

        if (sb.length() > 0) {
            throw new ImporterException("NIF nonroad import requires following file types \n" + sb.toString());
        }
    }

    private FormatUnit fileToDatasetTypeUnit(String key) {
        if ("ce".equals(key)) {
            return ceDatasetTypeUnit;
        }

        if ("em".equals(key)) {
            return emDatasetTypeUnit;
        }

        if ("ep".equals(key)) {
            return epDatasetTypeUnit;
        }

        if ("pe".equals(key)) {
            return peDatasetTypeUnit;
        }
        return null;
    }

}
