package com.flipkart.aesop.runtime.producer.schema.eventprocessor;

import java.util.Map;

import com.google.code.or.binlog.impl.event.QueryEvent;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import java.io.IOException;

/**
 * The <code>SchemaChangeEventProcessor</code> is a base interface for all schema change event processors
 * @author yogesh.dahiya
 */

public interface SchemaChangeEventProcessor
{

	void init() throws IOException;
	/** Handler method for schema change events */
	void process(QueryEvent queryEvent) throws Exception;

	void process(String databaseName, String tableName) throws Exception;

	/** Setter to pass the instace of SchemaRegistryService to work with */
	void setSchemaRegistryService(SchemaRegistryService schemaRegistryService);

	/** Setter to pass the tableUri to source name mapping */
	void setTableUriToSrcNameMap(Map<String, String> tableUriToSrcNameMap);

}
