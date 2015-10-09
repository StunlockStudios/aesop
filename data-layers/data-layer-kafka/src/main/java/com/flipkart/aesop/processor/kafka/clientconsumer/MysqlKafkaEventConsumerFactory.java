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
package com.flipkart.aesop.processor.kafka.clientconsumer;

import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusClusterInfo;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.stunlock.aesop.schemaregistry.ConfluentSchemaRegistryService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author kae
 */
public class MysqlKafkaEventConsumerFactory implements DbusClusterConsumerFactory {

	private String _schemaRegistryUrl;
	private KafkaClient _kafkaClient;

	public Collection<DatabusCombinedConsumer> createPartitionedConsumers(DbusClusterInfo dci, DbusPartitionInfo dpi) {
		ConfluentSchemaRegistryService.Config configBuilder = new ConfluentSchemaRegistryService.Config();
		configBuilder.registryUrl = getSchemaRegistryUrl();
		try {
			ConfluentSchemaRegistryService.StaticConfig schemaRegistryServiceConfig = configBuilder.build();
			ConfluentSchemaRegistryService schemaRegistryService = ConfluentSchemaRegistryService.build(schemaRegistryServiceConfig);
			DatabusCombinedConsumer consumer = new MysqlKafkaEventConsumer(schemaRegistryService, getKafkaClient());
			List<DatabusCombinedConsumer> list = new ArrayList<DatabusCombinedConsumer>();
			list.add(consumer);
			return list;
		} catch (InvalidConfigException exc) {
			return null;
		}
	}

	public void afterPropertiesSet() throws Exception {

	}

	/**
	 * @return the _schemaRegistryUrl
	 */
	public String getSchemaRegistryUrl() {
		return _schemaRegistryUrl;
	}

	/**
	 * @param _schemaRegistryUrl the _schemaRegistryUrl to set
	 */
	public void setSchemaRegistryUrl(String _schemaRegistryUrl) {
		this._schemaRegistryUrl = _schemaRegistryUrl;
	}

	/**
	 * @return the _kafkaClient
	 */
	public KafkaClient getKafkaClient() {
		return _kafkaClient;
	}

	/**
	 * @param _kafkaClient the _kafkaClient to set
	 */
	public void setKafkaClient(KafkaClient _kafkaClient) {
		this._kafkaClient = _kafkaClient;
	}

}
