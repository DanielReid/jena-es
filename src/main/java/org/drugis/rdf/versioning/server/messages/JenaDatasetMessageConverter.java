package org.drugis.rdf.versioning.server.messages;

import org.apache.jena.atlas.web.TypedInputStream;
import org.apache.jena.riot.*;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.drugis.rdf.versioning.server.Config;
import org.drugis.rdf.versioning.server.RequestParseException;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JenaDatasetMessageConverter extends AbstractHttpMessageConverter<DatasetGraph> {
  private static MediaType s_trig = MediaType.parseMediaType("text/trig");
  private static List<MediaType> s_supported = new ArrayList<MediaType>();

  static {
    // Add all supported languages
    for (Lang lang : RDFLanguages.getRegisteredLanguages()) {
      if (RDFLanguages.isQuads(lang)) {
        s_supported.add(MediaType.parseMediaType(lang.getContentType().getContentType()));
      }
    }
    // Make trig the default (if it exists)
    int idx = s_supported.indexOf(s_trig);
    if (idx > 0) {
      s_supported.set(idx, s_supported.get(0));
      s_supported.set(0, s_trig);
    }
  }

  @Override
  protected boolean supports(Class<?> clazz) {
    return DatasetGraph.class.isAssignableFrom(clazz);
  }

  @Override
  public List<MediaType> getSupportedMediaTypes() {
    return s_supported;
  }

  @Override
  protected MediaType getDefaultContentType(DatasetGraph t) throws IOException {
    return s_trig;
  }

  @Override
  protected DatasetGraph readInternal(Class<? extends DatasetGraph> clazz, HttpInputMessage inputMessage)
          throws IOException, HttpMessageNotReadableException {
    DatasetGraph ds = DatasetGraphFactory.createGeneral();
    try {
      StreamRDF dest = StreamRDFLib.dataset(ds);
      Lang lang = JenaGraphMessageConverter.determineRDFLang(inputMessage.getHeaders());
      TypedInputStream inputStream = TypedInputStream.wrap(inputMessage.getBody());
      if (inputStream.getContentType() != null) {
        lang = RDFLanguages.contentTypeToLang(inputStream.getMediaType());
      }
      RDFParser.create()
              .source(inputStream)
              .base(Config.BASE_URI)
              .lang(lang)
              .canonicalLiterals(true)
              .parse(dest);
    } catch (RiotException e) {
      throw new RequestParseException(e);
    }
    return ds;
  }

  @Override
  protected void writeInternal(DatasetGraph ds, HttpOutputMessage outputMessage)
          throws IOException, HttpMessageNotWritableException {
    RDFFormat fmt = JenaGraphMessageConverter.determineRDFFormat(outputMessage.getHeaders());
    try {
      RDFDataMgr.write(outputMessage.getBody(), ds, fmt);
    } finally {
      ds.end();
    }
  }
}