package gov.epa.emissions.commons.io.nif.point;

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

public class NIFPointDatasetTypeUnits {

    private FormatUnit ceDatasetTypeUnit;

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit epDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;

    private DatasetTypeUnit erDatasetTypeUnit;

    private DatasetTypeUnit euDatasetTypeUnit;

    private DatasetTypeUnit siDatasetTypeUnit;

    public NIFPointDatasetTypeUnits(SqlDataTypes sqlDataTypes) {
        FileFormat ceFileFormat = new NIFPointControlEquipmentFileFormat(sqlDataTypes);
        ceDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(ceFileFormat, sqlDataTypes), ceFileFormat,
                false);

        FileFormat emFileFormat = new NIFPointEmissionRecordsFileFormat(sqlDataTypes);
        emDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(emFileFormat, sqlDataTypes), emFileFormat,
                false);

        FileFormat epFileFormat = new NIFPointEmissionProcessFileFormat(sqlDataTypes);
        epDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(epFileFormat, sqlDataTypes), epFileFormat,
                false);

        FileFormat erFileFormat = new NIFPointEmissionReleasesFileFormat(sqlDataTypes);
        erDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(erFileFormat, sqlDataTypes), erFileFormat,
                false);

        FileFormat euFileFormat = new NIFPointEmissionUnitsFileFormat(sqlDataTypes);
        euDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(euFileFormat, sqlDataTypes), euFileFormat,
                false);

        FileFormat peFileFormat = new NIFPointEmissionPeriondsFileFormat(sqlDataTypes);
        peDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(peFileFormat, sqlDataTypes), peFileFormat,
                false);

        FileFormat siFileFormat = new NIFPointEmissionSitesFileFormat(sqlDataTypes);
        siDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(siFileFormat, sqlDataTypes), siFileFormat,
                false);
    }

    public void processFiles(InternalSource[] internalSources) throws ImporterException {
        associateFileWithUnit(internalSources);
        requiredExist();
    }

    public FormatUnit[] formatUnits() {
        return new FormatUnit[] { ceDatasetTypeUnit, emDatasetTypeUnit, epDatasetTypeUnit, erDatasetTypeUnit,
                euDatasetTypeUnit, peDatasetTypeUnit, siDatasetTypeUnit };
    }

    private void requiredExist() throws ImporterException {
        FormatUnit[] reqUnits = { emDatasetTypeUnit, epDatasetTypeUnit, erDatasetTypeUnit, euDatasetTypeUnit,
                peDatasetTypeUnit, siDatasetTypeUnit };
        for (int i = 0; i < reqUnits.length; i++) {
            if (reqUnits[i].getInternalSource() == null) {
                throw new ImporterException("The '" + reqUnits[i].fileFormat().identify()
                        + "' type file is required for importing NIF point");
            }
        }
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
            ceDatasetTypeUnit.setInternalSource(internalSource);
        }

        if ("em".equals(key)) {
            emDatasetTypeUnit.setInternalSource(internalSource);
        }

        if ("ep".equals(key)) {
            epDatasetTypeUnit.setInternalSource(internalSource);
        }

        if ("eu".equals(key)) {
            euDatasetTypeUnit.setInternalSource(internalSource);
        }

        if ("er".equals(key)) {
            erDatasetTypeUnit.setInternalSource(internalSource);
        }

        if ("pe".equals(key)) {
            ((DatasetTypeUnit) peDatasetTypeUnit).setInternalSource(internalSource);
        }

        if ("si".equals(key)) {
            siDatasetTypeUnit.setInternalSource(internalSource);
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
