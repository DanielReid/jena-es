package org.drugis.rdf.versioning.server;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class InvalidGraphSpecificationException extends RuntimeException {
	private static final long serialVersionUID = -3333929426939009961L;

	public InvalidGraphSpecificationException() {
		super("The request parameters must match either '?default' or '?graph={uri}'");
	}
}
