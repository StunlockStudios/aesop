package com.flipkart.aesop.processor.kafka.clientconsumer;

import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.stunlock.aesop.schemaregistry.ConfluentSchemaRegistryService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/*
 * Copyright 2015 kae.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 * @author kae
 */
public class MysqlKafkaEventConsumer implements DatabusCombinedConsumer {

	private static final Logger LOGGER = LogFactory.getLogger(MysqlKafkaEventConsumer.class);

	class ScnFuture {

		public Future _future;
		public SCN _scn;

		public ScnFuture(Future future, SCN scn) {
			_scn = scn;
			_future = future;
		}
	}

	class EventData {

		public byte[] _payload;
		public SchemaInfo _schema;

		public EventData(byte[] payload, SchemaInfo schema) {
			this._payload = payload;
			this._schema = schema;
		}
	}

	class SchemaInfo {

		public Schema _schema;
		public String[] _primaryKeys;

		public SchemaInfo(Schema schema, String[] primaryKeys) {
			this._schema = schema;
			this._primaryKeys = primaryKeys;
		}
	}

	private final KafkaClient _kafkaClient;
	private final ConfluentSchemaRegistryService _schemaRegistryService;
	private final GenericDatumReader<GenericRecord> _genericReader = new GenericDatumReader<GenericRecord>();
	private final GenericDatumWriter<Object> _genericWriter = new GenericDatumWriter<Object>();
	private final Map<Integer, SchemaInfo> _schemaInfoMap = new HashMap<Integer, SchemaInfo>();

	private final ArrayDeque<ScnFuture> _futureBuffer = new ArrayDeque<ScnFuture>();
	private final ArrayList<EventData> _batchBuffer = new ArrayList<EventData>();
	private BinaryDecoder _binDecoder = null;
	private BinaryEncoder _binEncoder = null;

	public MysqlKafkaEventConsumer(ConfluentSchemaRegistryService schemaRegistryService, KafkaClient kafkaClient) {
		this._schemaRegistryService = schemaRegistryService;
		this._kafkaClient = kafkaClient;
	}

	private ConsumerCallbackResult processEvent(DbusEvent dbusEvent, DbusEventDecoder eventDecoder) {
		ByteBuffer valueBuffer = dbusEvent.value();
		int schemaId = valueBuffer.getInt(0);
		valueBuffer.position(0);
		SchemaInfo schema;
		try {

			schema = getSchemaInfo(schemaId);
		} catch (DatabusException exc) {
			LOGGER.error(exc.toString());
			return ConsumerCallbackResult.ERROR;
		}
		byte[] valueBytes;
		if (valueBuffer.hasArray()) {
			valueBytes = valueBuffer.array();
		} else {
			valueBytes = new byte[valueBuffer.remaining()];
			valueBuffer.get(valueBytes);
		}
		_batchBuffer.add(new EventData(valueBytes, schema));
		return ConsumerCallbackResult.SUCCESS;
	}

	private SchemaInfo getSchemaInfo(int schemaId) throws DatabusException {
		SchemaInfo info = _schemaInfoMap.get(schemaId);
		if (info != null) {
			return info;
		}
		Schema schema = _schemaRegistryService.fetchSchemaById(schemaId);
		if (schema == null) {
			throw new DatabusException("Schema not found for id " + schemaId);
		}
		String primaryKeyFieldName = SchemaHelper.getMetaField(schema, "pk");
		String[] primaryKeys = primaryKeyFieldName.split(",");

		info = new SchemaInfo(schema, primaryKeys);
		_schemaInfoMap.put(schemaId, info);
		return info;
	}

	@Override
	public boolean canBootstrap() {
		return false;
	}

	@Override
	public ConsumerCallbackResult onStartConsumption() {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStopConsumption() {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartDataEventSequence(SCN scn) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndDataEventSequence(SCN scn) {
		int evtKeyToUse = 0;
		int evtPrimaryIndex = 0;
		for (int i = 0; i < _batchBuffer.size(); i++) {
			String[] primaryKeys = _batchBuffer.get(i)._schema._primaryKeys;
			for (int j = 0; j < primaryKeys.length; j++) {
				if (primaryKeys[j].equalsIgnoreCase("UserID")) {
					evtKeyToUse = i;
					evtPrimaryIndex = j;
					break;
				}
			}
		}
		ArrayList<GenericRecord> records = new ArrayList<GenericRecord>();
		for (int i =0; i < _batchBuffer.size(); i++) {
			EventData evt = _batchBuffer.get(0);
			_binDecoder = DecoderFactory.get().binaryDecoder(evt._payload, 4, evt._payload.length - 4, _binDecoder);
			_genericReader.setSchema(evt._schema._schema);
			_genericReader.setExpected(evt._schema._schema);
			try {
				GenericRecord record = _genericReader.read(null, _binDecoder);
				records.add(record);
			} catch (IOException ioex) {
				LOGGER.error("Error deserializing event: " + ioex.toString());
				return ConsumerCallbackResult.ERROR;
			}
		}
		GenericRecord keyRecord = records.get(evtKeyToUse);
		String keyName = _batchBuffer.get(evtPrimaryIndex)._schema._primaryKeys[evtPrimaryIndex];
		Object keyObj = keyRecord.get(keyName);
		Schema keySchema = ReflectData.get().getSchema(keyObj.getClass());
		ByteArrayOutputStream keyStream = new ByteArrayOutputStream();
		_binEncoder = EncoderFactory.get().binaryEncoder(keyStream, _binEncoder);
		_genericWriter.setSchema(keySchema);
		try

		{
		_genericWriter.write(keyObj, _binEncoder);
		} catch (IOException ioex) {
			LOGGER.error("Error serializing key: " + ioex);
			return ConsumerCallbackResult.ERROR;
		}
		byte[] keyArray = keyStream.toByteArray();
		for (EventData _batchBuffer1 : _batchBuffer) {
			Future future = _kafkaClient.getClient().send(new ProducerRecord("mysql", keyArray, _batchBuffer1._payload));
			_futureBuffer.add(new ScnFuture(future, scn));
		}
		_batchBuffer.clear();
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onRollback(SCN scn) {
		_batchBuffer.clear();
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartSource(String string, Schema schema) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndSource(String string, Schema schema) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent de, DbusEventDecoder ded) {
		return processEvent(de, ded);
	}

	@Override
	public ConsumerCallbackResult onCheckpoint(SCN scn) {
		while (_futureBuffer.isEmpty() == false) {
			ScnFuture future = _futureBuffer.peek();
			if (scn.compareTo(future._scn) > 0) {
				return ConsumerCallbackResult.SUCCESS;
			}
			try {
				RecordMetadata metadata = (RecordMetadata) future._future.get();
			} catch (ExecutionException eex) {
				LOGGER.error(eex.toString());
				return ConsumerCallbackResult.ERROR;
			} catch (InterruptedException exc) {
				LOGGER.error(exc.toString());
				return ConsumerCallbackResult.ERROR;
			}
			_futureBuffer.pop();
		}
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onError(Throwable thrwbl) {
		LOGGER.error(thrwbl.toString());
		return ConsumerCallbackResult.ERROR;
	}

	@Override
	public ConsumerCallbackResult onStartBootstrap() {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStopBootstrap() {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartBootstrapSequence(SCN scn) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndBootstrapSequence(SCN scn) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartBootstrapSource(String string, Schema schema) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndBootstrapSource(String string, Schema schema) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent de, DbusEventDecoder ded) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onBootstrapRollback(SCN scn) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onBootstrapCheckpoint(SCN scn) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onBootstrapError(Throwable thrwbl) {
		return ConsumerCallbackResult.SUCCESS;
	}
}
