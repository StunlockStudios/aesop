package com.stunlock.aesop.schemaregistry;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.springframework.beans.factory.InitializingBean;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

public class ConfluentSchemaRegistryService implements SchemaRegistryService, InitializingBean {

	private static final Logger LOGGER = LogFactory.getLogger(ConfluentSchemaRegistryService.class);
	private StaticConfig _config;
	private static final String _hackySchemaString = "{ "
		+ " \"doc\": \"Dummy string. Databus requires a schema for a source for some reason. We don't want a schema per source.\", "
		+ " \"name\": \"mysql\",  "
		+ " \"type\": \"record\",  "
		+ " \"fields\": [ ] "
		+ " }";
	public static byte[] HackySchemaId = SchemaId.createWithMd5(_hackySchemaString).getByteArray();

	private Map<Short, String> _hackySchemaMap;

	private CachedSchemaRegistryClient _registryClient;

	public ConfluentSchemaRegistryService(StaticConfig config) {
		_config = config;
		_hackySchemaMap = new HashMap<Short, String>();
		_hackySchemaMap.put((short) 1, _hackySchemaString);
		_registryClient = new CachedSchemaRegistryClient(_config.registryUrl, 500);
	}

	public static ConfluentSchemaRegistryService build(StaticConfig config) {
		return new ConfluentSchemaRegistryService(config);
	}

	@Override
	public void registerSchema(VersionedSchema vs) throws DatabusException {
		try {
			_registryClient.register(vs.getSchemaBaseName(), vs.getSchema());
			LOGGER.warn("Registered schema " + vs.getSchemaBaseName() + " --- " + vs.getSchema());
		} catch (IOException ioex) {
			throw new DatabusException(ioex);
		} catch (RestClientException restEx) {
			throw new DatabusException(restEx);
		}
	}

	@Override
	public String fetchSchema(String schemaIDString) throws NoSuchSchemaException, DatabusException {
		try {
			int schemaID = Integer.parseInt(schemaIDString);
			return _registryClient.getByID(schemaID).toString();
		} catch (NumberFormatException numex) {
			throw new NoSuchSchemaException(numex);
		} catch (IOException ioex) {
			throw new DatabusException(ioex);
		} catch (RestClientException restEx) {
			throw new DatabusException(restEx);
		}
	}

	@Override
	public String fetchLatestSchemaBySourceName(String name) throws NoSuchSchemaException, DatabusException {
		return fetchLatestVersionedSchemaBySourceName(name).getOrigSchemaStr();
	}

	@Override
	public VersionedSchema fetchLatestVersionedSchemaBySourceName(String name) throws NoSuchSchemaException, DatabusException {
		try {
			SchemaMetadata metadata = _registryClient.getLatestSchemaMetadata(name);
			Schema schema = _registryClient.getByID(metadata.getId());
			return new ConfluentVersionedSchema(metadata.getSchema(), (short) metadata.getVersion(), schema, schema.toString(), metadata.getId());
		} catch (IOException ioex) {
			throw new DatabusException(ioex);
		} catch (RestClientException restEx) {
			return null;
		}
	}

	@Override
	public Map<Short, String> fetchAllSchemaVersionsBySourceName(String name) throws NoSuchSchemaException, DatabusException {

		return _hackySchemaMap;
		//throw new UnsupportedOperationException(
		//	"fetchAllSchemaVersionsBySourceName not supported yet.");

	}

	@Override
	public SchemaId fetchSchemaIdForSourceNameAndVersion(String string, int i) throws DatabusException {
		throw new UnsupportedOperationException("fetchSchemaIdForSourceNameAndVersion not supported yet.");
	}

	@Override
	public void dropDatabase(String string) throws DatabusException {
		throw new UnsupportedOperationException("dropDatabase not supported yet.");
	}

	@Override
	public VersionedSchemaSet fetchAllMetadataSchemaVersions(short s) throws DatabusException {
		throw new UnsupportedOperationException("fetchAllMetadataSchemaVersions not supported yet.");
	}

	@Override
	public VersionedSchemaSet fetchAllMetadataSchemaVersions() throws DatabusException {
		throw new UnsupportedOperationException("fetchAllMetadataSchemaVersions not supported yet.");
	}

	public Schema fetchSchemaById(int id) throws DatabusException {
		try {
			return _registryClient.getByID(id);
		} catch (IOException ioex) {
			throw new DatabusException(ioex);
		} catch (RestClientException restEx) {
			return null;
		}
	}

	@Override
	public
		void afterPropertiesSet() throws Exception {

	}

	public static class Config {

		public String registryUrl;

		public Config() {
			this.registryUrl = null;
		}

		public StaticConfig build() throws InvalidConfigException {

			StaticConfig config = new StaticConfig();
			config.registryUrl = this.registryUrl;
			return config;
		}

	}

	public static class StaticConfig {

		public String registryUrl;

		public StaticConfig() {

		}
	}

}
