# extract-solr-schema
This is a golang tool to extract solr schema. The main steps are:

1. List all cores from solr context
2. Handle solr core one by one
	1. Get field definitions via schema.xml (old version) or schema API
	2. Select 100 documents and analysis document's fields type according field definition and if no definition are available, system will fallback to guess field type according golang json mapping and field name pattern.
