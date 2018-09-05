package org.drugis.rdf.versioning.server;

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb.TDBFactory;
import org.drugis.rdf.versioning.server.messages.BooleanResultMessageConverter;
import org.drugis.rdf.versioning.server.messages.JenaDatasetMessageConverter;
import org.drugis.rdf.versioning.server.messages.JenaGraphMessageConverter;
import org.drugis.rdf.versioning.server.messages.JenaResultSetMessageConverter;
import org.drugis.rdf.versioning.store.EventSource;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.jsr107.EhcacheCachingProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration.WebMvcAutoConfigurationAdapter;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.cache.Caching;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootConfiguration
@EnableCaching
public class Config extends WebMvcConfigurerAdapter {
  public static final String BASE_URI = "http://example.com/"; // FIXME
  @Value("${EVENT_SOURCE_URI_PREFIX}")
  private String uriPrefix;

  @Bean
  public CacheManager cacheManager() {
    long numberOfCacheItems = 1000;
    long ttl = 60*60*25;

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Object.class, Object.class, ResourcePoolsBuilder.heap(numberOfCacheItems))
            .withExpiry(Expirations.timeToLiveExpiration(new Duration(ttl, TimeUnit.SECONDS)))
            .build();

    Map<String, CacheConfiguration<?, ?>> caches = createCacheConfigurations(cacheConfiguration);

    EhcacheCachingProvider provider = (EhcacheCachingProvider) Caching.getCachingProvider();
    DefaultConfiguration configuration = new DefaultConfiguration(caches, provider.getDefaultClassLoader());
    return new JCacheCacheManager(provider.getCacheManager(provider.getDefaultURI(), configuration));
  }

  private Map<String, CacheConfiguration<?, ?>> createCacheConfigurations(CacheConfiguration<Object, Object> cacheConfiguration) {
    Map<String, CacheConfiguration<?, ?>> caches = new HashMap<>();
    caches.put("datasets", cacheConfiguration);
    caches.put("queryDataStore", cacheConfiguration);
    return caches;
  }


  @Bean
  public String datasetHistoryQuery() throws IOException {
    return getQueryResource("datasetHistory");
  }

  @Bean
  public String datasetInfoQuery() throws IOException {
    return getQueryResource("datasetInfo");
  }

  @Bean
  public String versionInfoQuery() throws IOException {
    return getQueryResource("versionInfo");
  }

  @Bean
  public String allMergedRevisionsQuery() throws IOException {
    return getQueryResource("allMergedRevisions");
  }

  @Bean
  public String currentMergedRevisionsQuery() throws IOException {
    return getQueryResource("currentMergedRevisions");
  }

  private String getQueryResource(String name) throws IOException {
    Resource resource = new ClassPathResource("/org/drugis/rdf/versioning/" + name + ".sparql");
    return FileCopyUtils.copyToString(new InputStreamReader(resource.getInputStream()));
  }

  @Bean
  public EventSource eventSource() {
    final DatasetGraph storage = TDBFactory.createDatasetGraph("DB");
    Runtime.getRuntime().addShutdownHook(new Thread(storage::close));
    return new EventSource(storage, uriPrefix);
  }

  @Bean
  CharacterEncodingFilter characterEncodingFilter() {
    CharacterEncodingFilter filter = new CharacterEncodingFilter();
    filter.setEncoding("UTF-8");
    filter.setForceEncoding(true);
    return filter;
  }

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
    converters.add(new JenaGraphMessageConverter());
    converters.add(new JenaDatasetMessageConverter());
    converters.add(new JenaResultSetMessageConverter());
    converters.add(new BooleanResultMessageConverter());
    super.configureMessageConverters(converters);
  }
}
