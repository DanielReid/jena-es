Jena Event Sourcing
===================

An implementation of the [Event Sourcing pattern](http://martinfowler.com/eaaDev/EventSourcing.html) for the [Apache Jena](https://jena.apache.org/) triple store. For more information on all components of the drugis project, please refer to the OVERALL-README.md in the root folder of the ADDIS-CORE project.

Running
-------

Set the URI prefix for the store, e.g.:

```
export EVENT_SOURCE_URI_PREFIX=http://localhost:8080
```

The run the application directly through Maven:

```
mvn spring-boot:run
```

Or package it (`mvn install spring-boot:repackage`) and then run it using `java -jar`.
