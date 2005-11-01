package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.temporal.FixedColsTableFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class NIFDatasetTypeUnits {
    
    private SqlDataTypes sqlDataTypes;

    private FormatUnit ceDatasetTypeUnit;

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit epDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;
    

    public NIFDatasetTypeUnits(SqlDataTypes sqlDataTypes){
        this.sqlDataTypes = sqlDataTypes;
        ceDatasetTypeUnit = ceDatasetTypeUnit();
        emDatasetTypeUnit = emDatasetTypeUnit();
        epDatasetTypeUnit = epDatasetTypeUnit();
        peDatasetTypeUnit = peDatasetTypeUnit();
    }
    
    public void processFiles(File[] files) throws ImporterException, IOException{
        associateFileWithUnit(files);
        requiredExist();
    }
    
    public FormatUnit controlEfficiencyUnit(){
        return ceDatasetTypeUnit;
    }
    
    public FormatUnit emissionsUnit(){
        return emDatasetTypeUnit;
    }
    
    public FormatUnit emissionsProcessUnit(){
        return epDatasetTypeUnit;
    }
    
    public FormatUnit periodsUnit(){
        return peDatasetTypeUnit;
    }

    private void requiredExist() throws ImporterException {
        if(((DatasetTypeUnit) emDatasetTypeUnit).getFile() ==null)
            throw new ImporterException("The emission file is required for importing NIF nonpoint");
        if(((DatasetTypeUnit) epDatasetTypeUnit).getFile() ==null)
            throw new ImporterException("The emission process is required for importing NIF nonpoint");
    }

    private void associateFileWithUnit(File[] files) throws IOException {
        for(int i=0;i<files.length; i++){
            File file = files[i];
            String key = notation(file);
            if(key.equals("ce")){
                ((DatasetTypeUnit) ceDatasetTypeUnit).setFile(file);
            }
            if(key.equals("em")){
                ((DatasetTypeUnit) emDatasetTypeUnit).setFile(file);
            }
            if(key.equals("ep")){
                ((DatasetTypeUnit) epDatasetTypeUnit).setFile(file);
            }
            if(key.equals("pe")){
                ((DatasetTypeUnit) peDatasetTypeUnit).setFile(file);
            }
        }
    }
    
    
    private String notation(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        String notation = null;
        while(line!=null){
            if(!line.startsWith("#") && line.length()>2){
                notation = line.substring(0,2).toLowerCase();
                break;
            }
        }
        reader.close();
        return notation;
    }

    private FormatUnit ceDatasetTypeUnit() {
        FileFormat fileFormat = new NIFAreaControlEfficiencyFileFormat(sqlDataTypes);
        return new DatasetTypeUnit(new FixedColsTableFormat(fileFormat,sqlDataTypes),fileFormat,false);
    }
    
    private FormatUnit emDatasetTypeUnit() {
        FileFormat fileFormat = new NIFAreaEmissionFileFormat(sqlDataTypes);
        return new DatasetTypeUnit(new FixedColsTableFormat(fileFormat,sqlDataTypes),fileFormat,false);
    }
    
    private FormatUnit epDatasetTypeUnit() {
        FileFormat fileFormat = new NIFAreaEmissionsProcessFileFormat(sqlDataTypes);
        return new DatasetTypeUnit(new FixedColsTableFormat(fileFormat,sqlDataTypes),fileFormat,false);
    }
    
    private FormatUnit peDatasetTypeUnit() {
        FileFormat fileFormat = new NIFAreaPeriodsFileFormat(sqlDataTypes);
        return new DatasetTypeUnit(new FixedColsTableFormat(fileFormat,sqlDataTypes),fileFormat,false);
    }

}
