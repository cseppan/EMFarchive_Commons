package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class NIFDatasetTypeUnitDelegate {
    

    public String notation(File file) throws ImporterException {
        String notation = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
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
    
    public InternalSource internalSource(String tablePrefix, String key, File file, FormatUnit formatUnit) {
        InternalSource internalSource = new InternalSource();
        internalSource.setSource(file.getAbsolutePath());
        internalSource.setType(formatUnit.fileFormat().identify());
        internalSource.setTable(tablePrefix+"_"+key);
        return internalSource;
    }


}
