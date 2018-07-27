package util;

import org.apache.jena.graph.*;
import org.apache.jena.graph.compose.Delta;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;
import org.drugis.rdf.versioning.server.Util;
import org.drugis.rdf.versioning.store.EventSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Schema upgrade tool for jena-es.
 * Allows a SPARQL query to be applied to all revisions of all graphs in all datasets managed in a jena-es datastore.
 * Rewrites history.
 */
public class SchemaUpgrade {
  // To keep cached pre- and post-upgrade content.
  private static class Previous<T> {
    T oldPrevious;
    T newPrevious;

    Previous(T oldPrevious, T newPrevious) {
      this.oldPrevious = oldPrevious;
      this.newPrevious = newPrevious;
    }
  }

  // Backlog of all revisions to convert, indexed by their previous revision.
  // A missing previous is represented by Node.ANY.
  private Map<Node, List<Node>> d_backlog = new HashMap<>();

  // Frontier of all revisions that have been upgraded, but are the previous to non-upgraded revisions.
  private Map<Node, Previous<Graph>> d_frontier = new HashMap<>();

  private DatasetGraph d_storage;

  private Graph d_versionInfo;

  private EventSource d_eventSource;

  private String d_query;

  public SchemaUpgrade(DatasetGraph storage, String uriPrefix, String query) {
    d_storage = storage;
    d_versionInfo = d_storage.getDefaultGraph();
    d_eventSource = new EventSource(storage, uriPrefix);
    d_query = query;
  }


  public void run() {
    ExtendedIterator<Triple> allRevisions = d_versionInfo.find(Node.ANY, RDF.Nodes.type, EventSource.esClassRevision);
    while (allRevisions.hasNext()) {
      Triple t = allRevisions.next();
      Node rev = t.getSubject();
      Node previous = Util.getUniqueOptionalObject(d_versionInfo.find(rev, EventSource.esPropertyPrevious, Node.ANY));
      addToBacklog(previous, rev);
    }

    // Iterate over the backlog
    while (backlogHasNext()) {
      Previous<Node> item = backlogNext();
      Node previous = item.oldPrevious,
              rev = item.newPrevious;
      System.out.println(rev + " -> " + previous + "; " + d_frontier.size());

      // Upgrade the revision and add it to the frontier
      Previous<Graph> frontierItem;
      if (!previous.equals(Node.ANY)) {
        Previous<Graph> previousGraphs = d_frontier.get(previous);
        frontierItem = upgradeRevision(rev, previousGraphs.oldPrevious, previousGraphs.newPrevious);
      } else {
        frontierItem = upgradeRevision(rev, GraphFactory.createGraphMem(), GraphFactory.createGraphMem());
      }
      d_frontier.put(rev, frontierItem);

      // Remove any unnecessary frontier items
      HashSet<Node> toRemove = new HashSet<>(d_frontier.keySet());
      toRemove.removeAll(d_backlog.keySet());
      for (Node rem : toRemove) {
        d_frontier.remove(rem);
      }
    }
  }

  // Upgrade a revision, given cached copies of the pre- and post-upgrade content of the previous revision
  private Previous<Graph> upgradeRevision(Node rev, Graph oldPrevious, Graph newPrevious) {
    // Calculate the old version of the revision
    Graph oldGraph = EventSource.applyRevision(d_storage, oldPrevious, rev);
    Graph bla = new GraphFactory().createGraphMem();
    GraphUtil.addInto(bla, oldGraph);
    // Upgrade it to the new version of the revision
    Delta delta = new Delta(newPrevious);
    delta.clear();
    GraphUtil.addInto(delta, oldGraph);
    upgradeGraph(delta);

    // Determine URIs for assertions/retractions
    String revId = rev.getURI().replaceFirst(d_eventSource.REVISION, "");
    Node asserts = NodeFactory.createURI(d_eventSource.ASSERT + revId);
    Node retracts = NodeFactory.createURI(d_eventSource.RETRACT + revId);

    // Remove old assertions, and write new ones
    d_storage.removeGraph(asserts);
    d_versionInfo.remove(rev, EventSource.esPropertyAssertions, asserts);
    if (!delta.getAdditions().isEmpty()) {
      d_storage.addGraph(asserts, d_eventSource.skolemize(delta.getAdditions()));
      d_versionInfo.add(new Triple(rev, EventSource.esPropertyAssertions, asserts));
    }

    // Remove old retractions, and write new ones
    d_storage.removeGraph(retracts);
    d_versionInfo.remove(rev, EventSource.esPropertyRetractions, retracts);
    if (!delta.getDeletions().isEmpty()) {
      d_storage.addGraph(retracts, d_eventSource.skolemize(delta.getDeletions()));
      d_versionInfo.add(new Triple(rev, EventSource.esPropertyRetractions, retracts));
    }

    // Copy new graph into cache
    Graph newGraph = GraphFactory.createGraphMem();
    GraphUtil.addInto(newGraph, delta);

    // Debug output
    RDFDataMgr.write(System.out, newGraph, Lang.TURTLE);

    return new Previous<>(bla, newGraph);
  }

  // Upgrade a graph
  private void upgradeGraph(Graph graph) {
    UpdateAction.parseExecute(d_query, graph);
  }

  private boolean backlogHasNext() {
    return !d_backlog.isEmpty();
  }

  private Previous<Node> backlogNext() {
    Node previous;

    // First try to exhaust any revisions that have a previous, as these would be consuming memory
    if (!d_frontier.isEmpty()) {
      previous = d_frontier.keySet().iterator().next();
    } else {
      previous = Node.ANY;
    }

    List<Node> list = d_backlog.get(previous); // can only fail if the data are corrupt
    Node next = list.remove(list.size() - 1);
    if (list.isEmpty()) {
      d_backlog.remove(previous);
    }
    return new Previous<>(previous, next);
  }

  private void addToBacklog(Node previous, Node revision) {
    if (previous == null) {
      previous = Node.ANY;
    }

    List<Node> list = d_backlog.computeIfAbsent(previous, k -> new ArrayList<>());

    list.add(revision);
  }

  public static void main(String[] args) throws IOException {
    final DatasetGraph storage = TDBFactory.createDatasetGraph("DB");
    String uriPrefix = System.getenv("EVENT_SOURCE_URI_PREFIX");
    if (uriPrefix == null || uriPrefix.isEmpty()) {
      System.err.println("You must set $EVENT_SOURCE_URI_PREFIX in the environment");
      System.exit(1);
    }

    if (args.length != 1) {
      System.err.println("You must supply the name of a SPARQL update query file");
    }

    String query = new String(Files.readAllBytes(Paths.get(args[0])));

    new SchemaUpgrade(storage, uriPrefix, query).run();
    storage.close();
  }
}
