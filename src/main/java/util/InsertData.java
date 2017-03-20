package util;

import java.util.Iterator;

import org.apache.jena.riot.RDFDataMgr;
import org.drugis.rdf.versioning.store.EventSource;

import jena.cmd.CmdException;
import jena.cmd.ArgDecl;
import arq.cmdline.CmdARQ;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;

public class InsertData extends CmdARQ {
    ArgDecl assemblerDescArg = new ArgDecl(ArgDecl.HasValue, "desc", "dataset") ;
	private DatasetGraph d_eventSource;
	private DatasetGraph d_data;
    
	protected InsertData(String[] argv) {
		super(argv);
		add(assemblerDescArg);
	}

	public static void main (String[] argv) {
        new InsertData(argv).mainRun() ;
	}

	@Override
	protected String getSummary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void exec() {
		Transactional tx = (Transactional) d_eventSource;
		tx.begin(ReadWrite.WRITE);
		for (Iterator<Node> it = d_data.listGraphNodes(); it.hasNext(); ) {
			Node graphName = it.next();
			d_eventSource.addGraph(graphName, d_data.getGraph(graphName));
		}
		tx.commit();
	}
	
	@Override
	protected void processModulesAndArgs() {
		if (!contains(assemblerDescArg)) {
			throw new CmdException("Please specify the assembler file using --desc=assembler-file.ttl");
		}
		Dataset ds = (Dataset) AssemblerUtils.build(getValue(assemblerDescArg), EventSource.esClassDataset.getURI());
		if (ds != null) {
			d_eventSource = ds.asDatasetGraph();
		} else {
			throw new CmdException("Could not assemble graph store.");
		}
		
		if (getPositional().size() != 1) {
			throw new CmdException("You must specify a data file.");
		}
		
        String filename = getPositionalArg(0);
		d_data = RDFDataMgr.loadDataset(filename).asDatasetGraph();
	}
}
