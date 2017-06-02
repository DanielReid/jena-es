package org.drugis.rdf.versioning.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.Syntax;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sparql.modify.UsingList;
import org.apache.jena.update.UpdateAction;
import org.drugis.rdf.versioning.store.DatasetGraphEventSourcing;
import org.drugis.rdf.versioning.store.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Controller
@RequestMapping("/datasets/{datasetId}/update")
public class UpdateController {
	@Autowired private EventSource d_eventSource;
	@Autowired private CacheManager cacheManager;

	Log d_log = LogFactory.getLog(getClass());

	@RequestMapping(method=RequestMethod.POST, consumes=WebContent.contentTypeSPARQLUpdate)
	public Object update(
			@PathVariable String datasetId,
			final HttpServletRequest request,
			@RequestParam(value="using-graph-uri", required=false) String[] usingGraphUri,
			@RequestParam(value="using-named-graph-uri", required=false) String[] usingNamedGraphUri,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			HttpServletResponse response)
			throws Exception { // TODO: request parameters
		d_log.debug("Update " + datasetId);
	
		final DatasetGraphEventSourcing dataset = Util.getDataset(d_eventSource, datasetId);
		final UsingList usingList = new UsingList();
		
		// Can not specify default of {} for @RequestParam.
		if (usingGraphUri == null) {
			usingGraphUri = new String[0];
		}
		if (usingNamedGraphUri == null) {
			usingNamedGraphUri = new String[0];
		}
		for (String uri : usingGraphUri) {
			usingList.addUsing(NodeFactory.createURI(uri));
		}
		for (String uri : usingNamedGraphUri) {
			usingList.addUsingNamed(NodeFactory.createURI(uri));
		}

		Runnable action = () -> {
            try {
                UpdateAction.parseExecute(usingList, dataset, request.getInputStream(), Config.BASE_URI, Syntax.syntaxARQ);
            } catch (QueryParseException e) {
                throw new RequestParseException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
		response.setHeader("X-EventSource-Version", newVersion);
		cacheManager.getCache("datasets").evict(datasetId);
		return null;
	}
}
