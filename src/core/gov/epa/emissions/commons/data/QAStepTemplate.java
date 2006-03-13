package gov.epa.emissions.commons.data;

public class QAStepTemplate {

    private long id;

    private String name;

    private String program;

    private String programArguments;

    private boolean required;

    private String order;

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

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

}
