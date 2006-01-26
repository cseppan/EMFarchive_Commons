package gov.epa.emissions.commons.io;

import java.io.Serializable;

public class Project implements Serializable {

    private long id;
    private String name;

    /*
     * Default constructor needed for hibernate and axis serialization
     */
    public Project() {
        super();
    }

    public Project(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof Project))
            return false;

        final Project project = (Project) other;

        if (!(project.getName().equals(this.getName())))
            return false;

        return true;
    }

    public int hashCode() {
        return name.hashCode();
    }

}
