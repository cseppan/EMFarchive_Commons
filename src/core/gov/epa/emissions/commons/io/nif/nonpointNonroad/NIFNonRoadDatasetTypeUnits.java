package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFDatasetTypeUnits;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

public abstract class NIFNonRoadDatasetTypeUnits implements NIFDatasetTypeUnits {

    protected FormatUnit ceDatasetTypeUnit;

    protected FormatUnit emDatasetTypeUnit;

    protected FormatUnit epDatasetTypeUnit;

    protected FormatUnit peDatasetTypeUnit;

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

    }

    public FormatUnit[] formatUnits() {
        return new FormatUnit[] { ceDatasetTypeUnit, emDatasetTypeUnit, epDatasetTypeUnit, peDatasetTypeUnit };
    }

    public String dataTable() {
        return emDatasetTypeUnit.getInternalSource().getTable();
    }

    protected void requiredExist() throws ImporterException {
        FormatUnit[] reqUnits = { emDatasetTypeUnit, epDatasetTypeUnit };
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < reqUnits.length; i++) {
            if (reqUnits[i].getInternalSource() == null) {
                sb.append("\t" + reqUnits[i].fileFormat().identify() + "\n");
            }
        }

        if (sb.length() > 0) {
            throw new ImporterException("NIF nonroad import requires following types \n" + sb.toString());
        }
    }

    protected FormatUnit keyToDatasetTypeUnit(String key) {
        key = key.toLowerCase();
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