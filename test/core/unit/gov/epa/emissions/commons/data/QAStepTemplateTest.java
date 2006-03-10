package gov.epa.emissions.commons.data;

import gov.epa.emissions.commons.data.QAStepTemplate;
import junit.framework.TestCase;

public class QAStepTemplateTest extends TestCase {

    public void testShouldHaveNameAsAttribute() {
        QAStepTemplate template = new QAStepTemplate();
        template.setName("name");

        assertEquals("name", template.getName());
    }

    public void testShouldHaveProgramAsAttribute() {
        QAStepTemplate template = new QAStepTemplate();
        template.setProgram("program");

        assertEquals("program", template.getProgram());
    }

    public void testShouldHaveProgramArgsAsAttribute() {
        QAStepTemplate template = new QAStepTemplate();
        template.setProgramArguments("args");

        assertEquals("args", template.getProgramArguments());
    }

    public void testShouldHaveRequiredAsAttribute() {
        QAStepTemplate template = new QAStepTemplate();
        template.setRequired(true);

        assertTrue(template.isRequired());
    }

    public void testShouldHaveOrderAsAttribute() {
        QAStepTemplate template = new QAStepTemplate();
        template.setOrder("order");

        assertEquals("order", template.getOrder());
    }

}
