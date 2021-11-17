package ar.edu.itba.graph.models;

import java.io.Serializable;

public class ParityVertexProp implements Serializable {
    
    private String  URL;
    private boolean parity;
    
    public ParityVertexProp(String uRL, boolean parity) {
        URL = uRL;
        this.parity = parity;
    }

    public String getURL() {
        return URL;
    }

    public void setURL(String uRL) {
        URL = uRL;
    }

    public boolean isParity() {
        return parity;
    }

    public void setParity(boolean parity) {
        this.parity = parity;
    }

    @Override
    public String toString() {
        return "ExtendedVertexProp [URL=" + URL + ", parity=" + parity + "]";
    }

    
}