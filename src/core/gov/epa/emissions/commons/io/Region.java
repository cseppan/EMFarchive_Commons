package gov.epa.emissions.commons.io;

import java.io.Serializable;

public class Region implements Serializable {

    private long id;
    private String name;

    /*
     * Default constructor needed for hibernate and axis serialization
     */
    public Region() {
        super();
    }

    public Region(String name){
        this.name=name;
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

        if (!(other instanceof Region))
            return false;

        final Region region = (Region) other;

        if (!(region.getName().equals(this.getName())))
            return false;

        return true;
    }

    public int hashCode() {
        return name.hashCode();
    }


}
