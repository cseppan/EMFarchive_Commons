package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;

import java.io.File;

public abstract class CompleteORLImporterTest_FIXME extends ORLImporterTestCase {

    protected void tearDown() throws Exception {
        Datasource ds = super.dbSetup.getDbServer().getEmissionsDatasource();
        ds.tableDefinition().deleteTable("arinv_nonpoint_nti99_nc_summary");
    }

    protected void doImport(String filename, Dataset dataset) throws Exception {
        Importer importer = new CompleteORLImporter(dbSetup.getDbServer(), true, super.types, dataset.getDatasetType());
        importer.preCondition(new File("test/data/orl/nc"), filename);
        importer.run(dataset);
    }

}
