package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFDatasetTypeUnits;
import gov.epa.emissions.commons.io.nif.NIFImportHelper;

public abstract class NIFNonPointDatasetTypeUnits implements NIFDatasetTypeUnits {

    protected FormatUnit ceDatasetTypeUnit;

    protected FormatUnit emDatasetTypeUnit;

    protected FormatUnit epDatasetTypeUnit;

    protected FormatUnit peDatasetTypeUnit;

    protected NIFImportHelper delegate;

    public NIFNonPointDatasetTypeUnits(SqlDataTypes sqlDataTypes, DataFormatFactory factory) {
        FileFormat ceFileFormat = new ControlEfficiencyFileFormat(sqlDataTypes, "NIF3.0 Nonpoint Control Efficiency");
        TableFormat ceTableFormat = factory.tableFormat(ceFileFormat, sqlDataTypes);
        ceDatasetTypeUnit = new DatasetTypeUnit(ceTableFormat, ceFileFormat, false);

        FileFormat emFileFormat = new EmissionRecordsFileFormat(sqlDataTypes, "NIF3.0 Nonpoint Emission Records");
        emDatasetTypeUnit = new DatasetTypeUnit(factory.tableFormat(emFileFormat, sqlDataTypes), emFileFormat, false);

        FileFormat epFileFormat = new EmissionProcessFileFormat(sqlDataTypes, "NIF3.0 Nonpoint Emission Process");
        epDatasetTypeUnit = new DatasetTypeUnit(factory.tableFormat(epFileFormat, sqlDataTypes), epFileFormat, false);

        FileFormat peFileFormat = new EmissionPeriodsFileFormat(sqlDataTypes, "NIF3.0 Nonpoint Emission Periods");
        peDatasetTypeUnit = new DatasetTypeUnit(factory.tableFormat(peFileFormat, sqlDataTypes), peFileFormat, false);

        delegate = new NIFImportHelper();
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
            throw new ImporterException("NIF nonpoint import requires following types \n" + sb.toString());
        }
    }

    protected FormatUnit keyToDatasetTypeUnit(String key) {
        if (key == null) {
            return null;
        }
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
