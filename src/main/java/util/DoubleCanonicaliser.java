package util;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.query.*;
import org.apache.jena.riot.process.normalize.CanonicalizeLiteral;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.TDBFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by daan on 10-7-17.
 */
public class DoubleCanonicaliser {

  private final DatasetGraph storage;

  public DoubleCanonicaliser(DatasetGraph storage) {
    this.storage = storage;
  }

  public void run() {
    QueryExecution queryExecution = QueryExecutionFactory.create("SELECT * { graph ?g { ?s ?p  ?val . filter(datatype(?val) = <http://www.w3.org/2001/XMLSchema#double> )}}", DatasetFactory.wrap(storage));
    ResultSet resultSet = queryExecution.execSelect();
    List<Pair<Quad, Quad>> pairArrayList = new ArrayList<>();
    while (resultSet.hasNext()) {
      QuerySolution next = resultSet.next();
      Node graph = next.getResource("g").asNode();
      Node subject = next.getResource("s").asNode();
      Node property = next.getResource("p").asNode();
      Node oldValue = next.getLiteral("val").asNode();
      Node newValue = CanonicalizeLiteral.get().apply(oldValue);
      Quad oldQuad = new Quad(graph, subject, property, oldValue);
      Quad newQuad = new Quad(graph, subject, property, newValue);
      pairArrayList.add(new Pair<>(newQuad, oldQuad));
    }
    pairArrayList.forEach(pair -> {
      storage.delete(pair.getRight());
      storage.add(pair.getLeft());
    });
  }

  public static void main(String[] args) throws IOException {
    final DatasetGraph storage = TDBFactory.createDatasetGraph("DB");
    new DoubleCanonicaliser(storage).run();
    storage.close();
  }
}
