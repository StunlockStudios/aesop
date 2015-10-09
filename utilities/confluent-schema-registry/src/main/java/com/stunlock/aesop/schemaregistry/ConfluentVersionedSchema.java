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
package com.stunlock.aesop.schemaregistry;

import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaId;
import org.apache.avro.Schema;

/**
 *
 * @author kae
 */
public class ConfluentVersionedSchema extends VersionedSchema {

	private int _globalId;

	public ConfluentVersionedSchema(VersionedSchemaId id, Schema s, String origSchemaStr, int globalID) {
		super(id, s, origSchemaStr);
		_globalId = globalID;
	}

	public ConfluentVersionedSchema(String baseName, short version, org.apache.avro.Schema s, String origSchemaStr, int globalID) {
		super(baseName, version, s, origSchemaStr);
		_globalId = globalID;
	}

	public int getGlobalId() {
		return _globalId;
	}

	public void setGlobalId(int _globalId) {
		this._globalId = _globalId;
	}
	
}
