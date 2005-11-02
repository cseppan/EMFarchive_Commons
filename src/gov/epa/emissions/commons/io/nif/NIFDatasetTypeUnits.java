package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.temporal.FixedColsTableFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class NIFDatasetTypeUnits {

    private FormatUnit ceDatasetTypeUnit;

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit epDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;

    public NIFDatasetTypeUnits(SqlDataTypes sqlDataTypes) {
        FileFormat ceFileFormat = new NIFAreaControlEfficiencyFileFormat(sqlDataTypes);
        ceDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(ceFileFormat, sqlDataTypes), ceFileFormat,
                false);

        FileFormat emFileFormat = new NIFAreaEmissionFileFormat(sqlDataTypes);
        emDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(emFileFormat, sqlDataTypes), emFileFormat,
                false);

        FileFormat epFileFormat = new NIFAreaEmissionsProcessFileFormat(sqlDataTypes);
        epDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(epFileFormat, sqlDataTypes), epFileFormat,
                false);

        FileFormat peFileFormat = new NIFAreaPeriodsFileFormat(sqlDataTypes);
        peDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(peFileFormat, sqlDataTypes), peFileFormat,
                false);
    }

    public void processFiles(InternalSource[] internalSources) throws ImporterException {
        associateFileWithUnit(internalSources);
        requiredExist();
    }

    public FormatUnit controlEfficiencyUnit() {
        return ceDatasetTypeUnit;
    }

    public FormatUnit emissionsUnit() {
        return emDatasetTypeUnit;
    }

    public FormatUnit emissionsProcessUnit() {
        return epDatasetTypeUnit;
    }

    public FormatUnit periodsUnit() {
        return peDatasetTypeUnit;
    }

    private void requiredExist() throws ImporterException {
        if (((DatasetTypeUnit) emDatasetTypeUnit).getInternalSource() == null)
            throw new ImporterException("The emission file is required for importing NIF nonpoint");
        if (((DatasetTypeUnit) epDatasetTypeUnit).getInternalSource() == null)
            throw new ImporterException("The emission process is required for importing NIF nonpoint");
    }

    private void associateFileWithUnit(InternalSource[] internalSources) throws ImporterException {
        for (int i = 0; i < internalSources.length; i++) {
            InternalSource internalSource = internalSources[i];
            String key = notation(internalSource);
            setFileToDatasetTypeUnit(internalSource, key);
        }
    }

    private void setFileToDatasetTypeUnit(InternalSource internalSource, String key) {
        if ("ce".equals(key)) {
            ((DatasetTypeUnit) ceDatasetTypeUnit).setInternalSource(internalSource);
        }

        if ("em".equals(key)) {
            ((DatasetTypeUnit) emDatasetTypeUnit).setInternalSource(internalSource);
        }

        if ("ep".equals(key)) {
            ((DatasetTypeUnit) epDatasetTypeUnit).setInternalSource(internalSource);
        }

        if ("pe".equals(key)) {
            ((DatasetTypeUnit) peDatasetTypeUnit).setInternalSource(internalSource);
        }
    }

    private String notation(InternalSource internalSource) throws ImporterException {
        String notation = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(internalSource.getSource()));
            String line = reader.readLine();
            while (line != null) {
                if (!line.startsWith("#") && line.length() > 2) {
                    notation = line.substring(0, 2).toLowerCase();
                    break;
                }
            }
            reader.close();
        } catch (IOException e) {
            throw new ImporterException(e.getMessage());
        }
        return notation;
    }

}
