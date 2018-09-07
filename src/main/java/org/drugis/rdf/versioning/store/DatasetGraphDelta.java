package org.drugis.rdf.versioning.store;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.compose.Delta;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.graph.GraphFactory;

/**
 * A read-write Dataset that tracks changes.
 */

public class DatasetGraphDelta extends DatasetGraphBase {
	
	private DatasetGraph d_next;
	private DatasetGraph d_base;
	private Map<Node, Delta> d_touched;

	public DatasetGraphDelta(DatasetGraph base) {
		d_base = base;
		d_next = DatasetGraphFactory.cloneStructure(d_base);
		d_touched = new HashMap<>();
	}
	
	/**
	 * @return A map from modified graphs to a Delta recording the changes.
	 */
	public Map<Node, Delta> getModifications() {
		return Collections.unmodifiableMap(d_touched);
	}

	private Graph touchGraph(Node graphName) {
		boolean def = graphName.equals(Quad.defaultGraphNodeGenerated);
		if (!d_touched.containsKey(graphName)) {
			Graph graph = def ? d_base.getDefaultGraph() : d_base.getGraph(graphName);
			if (graph == null) {
				graph = GraphFactory.createGraphMem();
			}
			Delta delta = new Delta(graph);
			if (def) {
				d_next.setDefaultGraph(delta);
			} else {
				d_next.addGraph(graphName, delta);
			}
			d_touched.put(graphName, delta);
		}
		return d_touched.get(graphName);
	}

	@Override
	public Graph getDefaultGraph() {
		return GraphView.createDefaultGraph(this);
	}

	@Override
	public Graph getGraph(Node graphNode) {
		return GraphView.createNamedGraph(this, graphNode);
	}

	@Override
	public boolean containsGraph(Node graphNode) {
		return d_next.containsGraph(graphNode);
	}

	@Override
	public void setDefaultGraph(Graph g) {
		addGraph(Quad.defaultGraphNodeGenerated, g);
	}
	
	@Override
	public void addGraph(Node graphName, Graph graph) {
        Graph target = touchGraph(graphName);
        target.clear();
        GraphUtil.addInto(target, graph);
	}

	@Override
	public void removeGraph(Node graphName) {
		touchGraph(graphName).clear();
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		return d_next.listGraphNodes();
	}

	@Override
	public void add(Quad quad) {
		touchGraph(quad.getGraph()).add(quad.asTriple());
	}

	@Override
	public void delete(Quad quad) {
		touchGraph(quad.getGraph()).delete(quad.asTriple());
	}

	@Override
	public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
		return d_next.find(g, s, p, o);
	}

	@Override
	public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
		return d_next.findNG(g, s, p, o);
	}

	@Override
	public boolean contains(Node g, Node s, Node p, Node o) {
		if (d_next.containsGraph(g)) { // work-around for JENA-792
			return d_next.contains(g, s, p, o);
		} else {
			return false;
		}
	}

	@Override
	public void clear() {
		d_next.clear();
	}

	@Override
	public boolean isEmpty() {
		return d_next.isEmpty();
	}

	@Override
	public long size() {
		return d_next.size();
	}

	@Override
	public boolean supportsTransactions() {
		return false;
	}

	@Override
	public void begin() {

	}

	@Override
	public void begin(TxnType txnType) {

	}

	@Override
	public void begin(ReadWrite readWrite) {

	}

	@Override
	public boolean promote(Promote promote) {
		return false;
	}

	@Override
	public void commit() {

	}

	@Override
	public void abort() {

	}

	@Override
	public void end() {

	}

	@Override
	public ReadWrite transactionMode() {
		return null;
	}

	@Override
	public TxnType transactionType() {
		return null;
	}

	@Override
	public boolean isInTransaction() {
		return false;
	}
}
