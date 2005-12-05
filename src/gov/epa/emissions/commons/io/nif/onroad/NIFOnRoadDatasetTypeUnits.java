package gov.epa.emissions.commons.io.nif.onroad;

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

public class NIFOnRoadDatasetTypeUnits implements NIFDatasetTypeUnits {

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;
    
    private FormatUnit trDatasetTypeUnit;


    public NIFOnRoadDatasetTypeUnits(SqlDataTypes sqlDataTypes) {
        FileFormat emFileFormat = new EmissionRecordsFileFormat(sqlDataTypes);
        emDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(emFileFormat, sqlDataTypes), emFileFormat,
                false);

        FileFormat peFileFormat = new EmissionPeriodsFileFormat(sqlDataTypes);
        peDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(peFileFormat, sqlDataTypes), peFileFormat,
                false);
        
        FileFormat trFileFormat = new TemporalRecordsFileFormat(sqlDataTypes);
        trDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(trFileFormat, sqlDataTypes), trFileFormat,
                false);
    }

    public void processFiles(InternalSource[] internalSources, String tableName) throws ImporterException {
        associateFileWithUnit(internalSources, tableName);
        requiredExist();
    }

    public FormatUnit[] formatUnits() {
        return new FormatUnit[] {emDatasetTypeUnit, peDatasetTypeUnit, trDatasetTypeUnit };
    }

    private void associateFileWithUnit(InternalSource[] internalSources, String tableName) throws ImporterException {
        for (int i = 0; i < internalSources.length; i++) {
            InternalSource internalSource = internalSources[i];
            String key = notation(internalSource);
            FormatUnit formatUnit = fileToDatasetTypeUnit(key);
            if(formatUnit!=null){
                internalSource.setType(formatUnit.fileFormat().identify());
                internalSource.setTable(tableName+"_nif_"+key);
                formatUnit.setInternalSource(internalSource);
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
            throw new ImporterException("NIF onroad import requires following file types \n" + sb.toString());
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
