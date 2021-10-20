package ar.edu.itba.graph.models;

public class VertexProp {
    private String URL;
    private String creator;

    public VertexProp(String anUrl, String aCreator) {
        URL = anUrl;
        creator = aCreator;
    }

    public String getURL() {
        return URL;
    }

    public String getCreator() {
        return creator;
    }
}