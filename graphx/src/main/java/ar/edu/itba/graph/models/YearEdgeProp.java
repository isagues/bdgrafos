package ar.edu.itba.graph.models;

import java.io.Serializable;

public class YearEdgeProp implements Serializable {
    
    private String label;
    private String year;

    public YearEdgeProp(String aLabel, String year) {
        label = aLabel;
        this.year = year;
    }

    public String getLabel() {
        return label;
    }

    public String getDate() {
        return year;
    }

    @Override
    public String toString() {
        return "EdgeProp [year=" + year + ", label=" + label + "]";
    }
}