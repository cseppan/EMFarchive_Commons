package gov.epa.emissions.commons.data;

public class QAStepTemplate {

    private long listIndex;

    private String name;

    private String program;

    private String programArguments;

    private boolean required;

    private float order;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getProgram() {
        return program;
    }

    public void setProgram(String program) {
        this.program = program;
    }

    public void setProgramArguments(String args) {
        this.programArguments = args;
    }

    public String getProgramArguments() {
        return programArguments;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean optional) {
        this.required = optional;
    }

    public float getOrder() {
        return order;
    }

    public void setOrder(float order) {
        this.order = order;
    }

    public long getListIndex() {
        return listIndex;
    }

    public void setListIndex(long listIndex) {
        this.listIndex = listIndex;
    }

}
