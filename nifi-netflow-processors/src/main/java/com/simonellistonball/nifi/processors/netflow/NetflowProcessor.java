/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.simonellistonball.nifi.processors.netflow;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.DatagramChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;

import com.simonellistonball.nifi.processors.netflow.NetflowProcessor.NetflowEvent;

@Tags({ "netflow", "networking", "listen", "metron" })
@CapabilityDescription("Listens to Cisco Netflow and outputs a JSON representation")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class NetflowProcessor extends AbstractListenEventBatchingProcessor<NetflowEvent> {

	public class NetflowRecord extends HashMap<String, String> {
		private static final long serialVersionUID = 1L;
	}

	public static final PropertyDescriptor TEMPLATE_CACHE = new PropertyDescriptor.Builder()
			.name("Template cache service").displayName("Template cache service")
			.description("A distributed cache service which handles the templates provided in Netflow 9").required(true)
			.identifiesControllerService(DistributedMapCacheClient.class).build();

	public static final Relationship REL_INVALID = new Relationship.Builder().name("Invalid")
			.description("Invalid packets received").build();

	@Override
	protected List<PropertyDescriptor> getAdditionalProperties() {
		return Arrays.asList(TEMPLATE_CACHE);
	}

	@Override
	protected List<Relationship> getAdditionalRelationships() {
		return Arrays.asList(REL_INVALID);
	}

	public static final String UDP_PORT_ATTR = "udp.port";
	public static final String UDP_SENDER_ATTR = "udp.sender";

	@Override
	protected Map<String, String> getAttributes(
			AbstractListenEventBatchingProcessor<NetflowEvent>.FlowFileEventBatch batch) {
		final String sender = batch.getEvents().get(0).getSender();
		final Map<String, String> attributes = new HashMap<>(3);
		attributes.put(UDP_SENDER_ATTR, sender);
		attributes.put(UDP_PORT_ATTR, String.valueOf(port));
		return attributes;
	}

	@Override
	protected String getTransitUri(AbstractListenEventBatchingProcessor<NetflowEvent>.FlowFileEventBatch batch) {
		final String sender = batch.getEvents().get(0).getSender();
		final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
		final String transitUri = new StringBuilder().append("udp").append("://").append(senderHost).append(":")
				.append(port).toString();
		return transitUri;
	}

	@Override
	protected ChannelDispatcher createDispatcher(ProcessContext context, BlockingQueue<NetflowEvent> events)
			throws IOException {
		final Integer bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
		final BlockingQueue<ByteBuffer> bufferPool = createBufferPool(context.getMaxConcurrentTasks(), bufferSize);
		final DistributedMapCacheClient templateCache = context.getProperty(TEMPLATE_CACHE)
				.asControllerService(DistributedMapCacheClient.class);
		final EventFactory<NetflowEvent> eventFactory = new NetflowEventFactory(templateCache);
		return new DatagramChannelDispatcher<>(eventFactory, bufferPool, events, getLogger());
	}

	@SuppressWarnings("rawtypes")
	static class NetflowEvent extends StandardEvent {

		private List<NetflowRecord> records;

		@SuppressWarnings("unchecked")
		public NetflowEvent(String sender, byte[] data, ChannelResponder responder, List<NetflowRecord> records) {
			super(sender, data, responder);
			this.records = records;
		}

		public List<NetflowRecord> getRecords() {
			return records;
		}
	}

	public class NetflowTemplateKey {
		private String sender;

		protected String getSender() {
			return sender;
		}

		protected int getTemplate() {
			return template;
		}

		private int template;

		public NetflowTemplateKey(String sender, int template) {
			super();
			this.sender = sender;
			this.template = template;
		}

	}

	private class NetflowEventFactory implements EventFactory<NetflowEvent> {
		public class Field {
			private short type;
			private short length;
			private String name;

			protected short getType() {
				return type;
			}

			protected short getLength() {
				return length;
			}

			public Field(short type, short length) {
				super();
				this.type = type;
				this.length = length;
				this.name = typeToName(type);
			}

			private String typeToName(short type) {
				return "";
			}

			/**
			 * Return the name of the field, which is used to translate field numbers into
			 * human readable names.
			 * 
			 * @return String name of the field
			 */
			public String getName() {
				return name;
			}

		}

		public class NetflowTemplate extends LinkedList<Field> {
			private short id;

			public NetflowTemplate(short id) {
				super();
				this.id = id;
			}

			protected short getId() {
				return id;
			}

			private static final long serialVersionUID = 1L;
		}

		private final DistributedMapCacheClient templateCache;
		private final Map<NetflowTemplateKey, NetflowTemplate> localCache = new HashMap<NetflowTemplateKey, NetflowTemplate>();

		private final Serializer<NetflowTemplateKey> keySerializer = new Serializer<NetflowTemplateKey>() {

			@Override
			public void serialize(NetflowTemplateKey value, OutputStream output)
					throws SerializationException, IOException {
				output.write(value.getTemplate());
				output.write(value.getSender().getBytes());
			}
		};
		private final Deserializer<NetflowTemplate> valueDeserializer = new Deserializer<NetflowTemplate>() {

			@Override
			public NetflowTemplate deserialize(byte[] input) throws DeserializationException, IOException {
				return new NetflowTemplate((short) 1);
			}

		};

		private final Serializer<NetflowTemplate> valueSerializer = new Serializer<NetflowTemplate>() {

			@Override
			public void serialize(NetflowTemplate value, OutputStream output)
					throws SerializationException, IOException {
				// TODO Auto-generated method stub

			}
		};

		public NetflowEventFactory(DistributedMapCacheClient templateCache) {
			super();
			this.templateCache = templateCache;
		}

		@Override
		public NetflowEvent create(byte[] data, Map<String, String> metadata, ChannelResponder responder) {
			String sender = null;
			if (metadata != null && metadata.containsKey(EventFactory.SENDER_KEY)) {
				sender = metadata.get(EventFactory.SENDER_KEY);
			}

			List<NetflowRecord> flowRecords = new LinkedList<NetflowRecord>();
			// what have we got?

			// version

			ByteBuffer bb = ByteBuffer.wrap(data);

			int version = bb.getInt();
			if (version == 9) {
				int count = bb.getShort();
				int uptime = bb.getInt();
				int unixtime = bb.getInt();
				int sequence = bb.getInt();
				int sourceid = bb.getInt();

				for (int i = 0; i < count; i++) {
					short flowsetId = bb.getShort();
					short length = bb.getShort();
					ByteBuffer record = ByteBuffer.wrap(data, bb.position(), length);

					if (flowsetId == 0) {
						// if templates, put in cache and maintain a local copy
						NetflowTemplate template = processTemplates(record);
						NetflowTemplateKey key = new NetflowTemplateKey(String.valueOf(sourceid), template.getId());
						cachePut(key, template);
						getLogger().debug("Received a Flowset Template from %s, with id %d",
								new Object[] { key.getSender(), key.getTemplate() });
					} else if (flowsetId == 1) {
						// this is an options packet, skip it
						bb.position(bb.position() + length - 4);

						getLogger().debug("Received an Options Template from %s, with id %d",
								new Object[] { String.valueOf(sourceid) });
					} else if (flowsetId > 255) {
						// if records, check the local then distributed cache for templates
						NetflowTemplateKey key = new NetflowTemplateKey(String.valueOf(sourceid), flowsetId);
						NetflowTemplate template;
						template = cacheGet(key);
						if (template == null) {
							// requeue the packet until the template arrives
						} else {
							flowRecords.add(processFlowRecord(record, template));
						}
					}
				}
			}
			return new NetflowEvent(sender, data, responder, flowRecords);
		}

		private NetflowTemplate cacheGet(NetflowTemplateKey key) {
			if (localCache.containsKey(key)) {
				return localCache.get(key);
			} else {
				try {
					return templateCache.get(key, keySerializer, valueDeserializer);
				} catch (IOException e) {
					return null;
				}
			}
		}

		private void cachePut(NetflowTemplateKey key, NetflowTemplate value) {
			localCache.put(key, value);
			try {
				templateCache.put(key, value, keySerializer, valueSerializer);
			} catch (IOException e) {
				getLogger().error("Failed to write template to distributed cache", e);
			}
		}

		private NetflowTemplate processTemplates(ByteBuffer bb) {
			short id = bb.getShort();
			short fieldCount = bb.getShort();
			NetflowTemplate template = new NetflowTemplate(id);
			for (short i = 0; i < fieldCount; i++) {
				short field = bb.getShort();
				short length = bb.getShort();
				template.add(new Field(field, length));
			}
			return template;
		}

		private NetflowRecord processFlowRecord(ByteBuffer bb, NetflowTemplate template) {
			Map<String, String> record = new NetflowRecord();
			for (Field item : template) {
				short length = item.getLength();
				byte[] dst = new byte[length];
				bb.get(dst, 0, length);
				// TODO - do encoding properly
				record.put(item.getName(), new String(dst));
			}
			return null;
		}
	}

}
