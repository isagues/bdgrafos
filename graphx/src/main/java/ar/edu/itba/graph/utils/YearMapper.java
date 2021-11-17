package ar.edu.itba.graph.utils;

import java.io.Serializable;

import org.apache.spark.graphx.Edge;

import ar.edu.itba.graph.models.EdgeProp;
import ar.edu.itba.graph.models.YearEdgeProp;
import scala.runtime.AbstractFunction1;

public class YearMapper extends AbstractFunction1<Edge<EdgeProp>, YearEdgeProp>  implements Serializable {

    @Override
    public YearEdgeProp apply(Edge<EdgeProp> prev) {
        
        return new YearEdgeProp(
            prev.attr.getLabel(), 
            String.valueOf(prev.attr.getDate().getYear() + 1900)
        );
    }

}
