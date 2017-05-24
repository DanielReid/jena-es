import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.riot.RDFDataMgr;
import org.drugis.rdf.versioning.store.EventSource;
import org.junit.Before;
import org.junit.Test;
import util.SchemaUpgrade;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.junit.Assert.*;

/**
 * Created by joris on 17-3-17.
 */
public class SchemaUpgradeTest {


  private static final String ES="http://drugis.org/eventSourcing/es#",
          DATASET="http://example.com/datasets/",
          VERSION="http://example.com/versions/",
          REVISION="http://example.com/revisions/",
          FOAF="http://xmlns.com/foaf/0.1/",
          DCTERMS="http://purl.org/dc/elements/1.1/",
          ID_GOBLIN_DATASET = "ubb245f8sz",
          ID_SPIDER_DATASET = "qmk2x16nz1",
          ID_GOBLIN_VERSION0 = "3ucq3j5c7u",
          ID_GOBLIN_VERSION1 = "7wi4xglx1c",
          ID_SPIDER_VERSION0 = "f98gj2sgsn",
          ID_SPIDER_VERSION1 = "g05ri5qvvq",
          ID_REV1 = "38fc1de7a-43ea-11e4-a12c-3314171ce0bb",
          ID_REV2 = "302431f4-43e8-11e4-8745-c72e64fa66b1",
          ID_REV3 = "44ea0618-43e8-11e4-bcfb-bba47531d497";

  private static final Node FOAF_PERSON = createURI(FOAF + "Person");
  private static final Node DCTERMS_TITLE = createURI(DCTERMS + "title");
  private static final Node FOAF_KNOWS = createURI(FOAF + "knows");
  private static final Node SPIDERMAN = createURI("http://example.com/Spiderman");
  private static final Node PETER_PARKER = createURI("http://example.com/PeterParker");

  private DatasetGraph d_datastore;
  private EventSource d_eventSource;

  private Node d_goblinDatasetUri;
  private Node d_spiderDatasetUri;
  private Node d_goblinV0Uri;
  private Node d_goblinV1Uri;
  private Node d_spiderV0Uri;
  private Node d_spiderV1Uri;
  private Node d_rev1Uri;
  private Node d_rev2Uri;
  private Node d_rev3Uri;


  @Before
  public void setUp() throws Exception {
    d_datastore = RDFDataMgr.loadDataset("data.trig").asDatasetGraph();
    d_eventSource = new EventSource(d_datastore, "http://example.com/");
    d_goblinDatasetUri = createURI(DATASET + ID_GOBLIN_DATASET);
    d_spiderDatasetUri = createURI(DATASET + ID_SPIDER_DATASET);
    d_goblinV0Uri = createURI(VERSION + ID_GOBLIN_VERSION0);
    d_goblinV1Uri = createURI(VERSION + ID_GOBLIN_VERSION1);
    d_spiderV0Uri = createURI(VERSION + ID_SPIDER_VERSION0);
    d_spiderV1Uri = createURI(VERSION + ID_SPIDER_VERSION1);
    d_rev1Uri = createURI(REVISION + ID_REV1);
    d_rev2Uri = createURI(REVISION + ID_REV2);
    d_rev3Uri = createURI(REVISION + ID_REV3);
  }

  @Test
  public void test() {
    assertTrue(d_datastore.getDefaultGraph().contains(d_goblinDatasetUri, RDF.Nodes.type, createURI(ES + "Dataset")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_spiderDatasetUri, RDF.Nodes.type, createURI(ES + "Dataset")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_goblinV0Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_goblinV1Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_spiderV0Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_spiderV1Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_rev1Uri, RDF.Nodes.type, createURI(ES + "Revision")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_rev2Uri, RDF.Nodes.type, createURI(ES + "Revision")));
    assertTrue(d_datastore.getDefaultGraph().contains(d_rev3Uri, RDF.Nodes.type, createURI(ES + "Revision")));
  }

  @Test
  public void testSchemaUpgrade() throws IOException {
    String query = new String(Files.readAllBytes(Paths.get("testQuery1.sparql")));
    SchemaUpgrade schemaUpgrade = new SchemaUpgrade(d_datastore,"http://example.com", query);
    schemaUpgrade.run();
    Graph base = GraphFactory.createGraphMem();
    Graph graph = EventSource.applyRevision(d_datastore, base, d_rev1Uri);
    checkGraphRev1(graph);
    Graph graph2 = EventSource.applyRevision(d_datastore, graph, d_rev3Uri);
    checkGraphRev3(graph2);
  }

  private void checkGraphRev1(Graph graph) {
    assertTrue(graph.contains(PETER_PARKER, RDF.Nodes.type, FOAF_PERSON));
    assertTrue(graph.contains(PETER_PARKER, DCTERMS_TITLE, createLiteral("Spiderman")));
    assertTrue(graph.contains(PETER_PARKER, DCTERMS_TITLE, createLiteral("Peter Parker")));
    assertEquals(3, graph.size());
  }

  private void checkGraphRev3(Graph graph) {
    assertTrue(graph.contains(PETER_PARKER, RDF.Nodes.type, FOAF_PERSON));
    assertTrue(graph.contains(PETER_PARKER, DCTERMS_TITLE, createLiteral("Peter Parker")));
    assertTrue(graph.contains(PETER_PARKER, createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
    assertEquals(3, graph.size());
  }
}