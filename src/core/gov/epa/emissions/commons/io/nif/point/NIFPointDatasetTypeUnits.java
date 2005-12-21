package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFDatasetTypeUnits;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class NIFPointDatasetTypeUnits implements NIFDatasetTypeUnits{

    private FormatUnit ceDatasetTypeUnit;

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit epDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;

    private DatasetTypeUnit erDatasetTypeUnit;

    private DatasetTypeUnit euDatasetTypeUnit;

    private DatasetTypeUnit siDatasetTypeUnit;

    public NIFPointDatasetTypeUnits(SqlDataTypes sqlDataTypes) {
        FileFormat ceFileFormat = new ControlEquipmentFileFormat(sqlDataTypes);
        ceDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(ceFileFormat, sqlDataTypes), ceFileFormat,
                false);

        FileFormat emFileFormat = new EmissionRecordsFileFormat(sqlDataTypes);
        emDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(emFileFormat, sqlDataTypes), emFileFormat,
                false);

        FileFormat epFileFormat = new EmissionProcessFileFormat(sqlDataTypes);
        epDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(epFileFormat, sqlDataTypes), epFileFormat,
                false);

        FileFormat erFileFormat = new EmissionReleasesFileFormat(sqlDataTypes);
        erDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(erFileFormat, sqlDataTypes), erFileFormat,
                false);

        FileFormat euFileFormat = new EmissionUnitsFileFormat(sqlDataTypes);
        euDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(euFileFormat, sqlDataTypes), euFileFormat,
                false);

        FileFormat peFileFormat = new EmissionPeriodsFileFormat(sqlDataTypes);
        peDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(peFileFormat, sqlDataTypes), peFileFormat,
                false);

        FileFormat siFileFormat = new EmissionSitesFileFormat(sqlDataTypes);
        siDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(siFileFormat, sqlDataTypes), siFileFormat,
                false);
    }

    public void processFiles(InternalSource[] internalSources, String tableName) throws ImporterException {
        associateFileWithUnit(internalSources, tableName);
        requiredExist();
    }

    public FormatUnit[] formatUnits() {
        return new FormatUnit[] { ceDatasetTypeUnit, emDatasetTypeUnit, epDatasetTypeUnit, erDatasetTypeUnit,
                euDatasetTypeUnit, peDatasetTypeUnit, siDatasetTypeUnit };
    }

    private void requiredExist() throws ImporterException {
        FormatUnit[] reqUnits = { emDatasetTypeUnit, epDatasetTypeUnit, erDatasetTypeUnit, euDatasetTypeUnit,
                peDatasetTypeUnit, siDatasetTypeUnit };
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < reqUnits.length; i++) {
            if (reqUnits[i].getInternalSource() == null) {
                sb.append("\t" + reqUnits[i].fileFormat().identify() + "\n");
            }
        }

        if (sb.length() > 0) {
            throw new ImporterException("NIF point import requires following file types \n" + sb.toString());
        }
    }

    private void associateFileWithUnit(InternalSource[] internalSources, String tableName) throws ImporterException {
        for (int i = 0; i < internalSources.length; i++) {
            InternalSource internalSource = internalSources[i];
            String key = notation(internalSource);
            FormatUnit formatUnit = fileToDatasetTypeUnit(key);
            if(formatUnit!=null){
                internalSource.setType(formatUnit.fileFormat().identify());
                internalSource.setTable(tableName+"_"+key);
                formatUnit.setInternalSource(internalSource);
            }
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

        if ("eu".equals(key)) {
            return euDatasetTypeUnit;
        }

        if ("er".equals(key)) {
            return erDatasetTypeUnit;
        }

        if ("pe".equals(key)) {
            return peDatasetTypeUnit;
        }

        if ("si".equals(key)) {
            return siDatasetTypeUnit;
        }
        return null;
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
