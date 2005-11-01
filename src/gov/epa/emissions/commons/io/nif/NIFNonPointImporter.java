package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NIFNonPointImporter {

    private NIFImporter delegate;

    private SqlDataTypes sqlDataTypes;

    private NIFDatasetTypeUnits units;

    public NIFNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new NIFImporter(datasource, sqlDataTypes);
        units = new NIFDatasetTypeUnits(sqlDataTypes);

    }

    private void preImport(Dataset dataset) throws ImporterException, IOException {
        List internalSources = Arrays.asList(dataset.getInternalSources());
        List files = new ArrayList();
        for (int i = 0; i < internalSources.size(); i++) {
            InternalSource source = (InternalSource) internalSources.get(i);
            File file = new File(source.getSource());
            files.add(file);
        }
        units.processFiles((File[]) files.toArray(new File[0]));

    }

    public void run(Dataset dataset) throws ImporterException, IOException {
        preImport(dataset);
        try{
            doImportCE(dataset);
        }catch(ImporterException e){
            
            throw e;
        }
    }

    private void doImportCE(Dataset dataset) throws ImporterException {
        DatasetTypeUnit typeUnit = (DatasetTypeUnit) units.controlEfficiencyUnit();
        File file = typeUnit.getFile();
        String tableName = dataset.getName()+"_ce";
        delegate.run(file,tableName,typeUnit,dataset);
    }

}
