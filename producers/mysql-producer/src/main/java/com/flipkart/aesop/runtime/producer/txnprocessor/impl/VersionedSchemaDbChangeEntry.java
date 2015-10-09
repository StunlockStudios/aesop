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
package com.flipkart.aesop.runtime.producer.txnprocessor.impl;

import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.KeyPair;
import com.linkedin.databus2.schemas.VersionedSchema;
import java.util.List;
import org.apache.avro.generic.GenericRecord;

public class VersionedSchemaDbChangeEntry extends DbChangeEntry {

	private VersionedSchema _versionedSchema;

	public VersionedSchemaDbChangeEntry(long scn, long timestampNanos, GenericRecord record, DbusOpcode opCode, boolean isReplicated, VersionedSchema schema, List<KeyPair> pkeys) {
		super(scn, timestampNanos, record, opCode, isReplicated, schema.getSchema(), pkeys);
		_versionedSchema = schema;
	}

	public VersionedSchema getVersionedSchema() {
		return _versionedSchema;
	}

	public void setVersionedSchema(VersionedSchema _versionedSchema) {
		this._versionedSchema = _versionedSchema;
	}
	
}
