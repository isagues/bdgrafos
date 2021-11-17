package ar.edu.itba.graph.utils;

import java.io.Serializable;

import ar.edu.itba.graph.models.ParityVertexProp;
import ar.edu.itba.graph.models.VertexProp;
import scala.runtime.AbstractFunction2;

public class ParityMapper extends AbstractFunction2<Object, VertexProp, ParityVertexProp> implements Serializable {

    @Override
    public ParityVertexProp apply(Object id, VertexProp prev) {
        return new ParityVertexProp(prev.getURL(), ((Long) id) % 2 == 0);
    }
}
