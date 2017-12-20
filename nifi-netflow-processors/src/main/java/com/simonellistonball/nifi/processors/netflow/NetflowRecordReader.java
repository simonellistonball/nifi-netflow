package com.simonellistonball.nifi.processors.netflow;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * 
 * Assumes that the inbound flow file contains a single netflow packet, which
 * may represent a large number of records.
 * 
 * This implementation processes as entire netflow packet as a single unit, and
 * as such is not a reusable reader.
 * 
 * @author sball
 *
 */
public class NetflowRecordReader implements RecordReader {
	private final DataInputStream dis;
	private final RecordSchema schema;
	private final ComponentLog logger;

	private Map<NetflowTemplateKey, NetflowTemplate> templates = new HashMap<NetflowTemplateKey, NetflowTemplate>();
	private Queue<NetflowRecord> records = new ConcurrentLinkedQueue<NetflowRecord>();

	private ComponentLog getLogger() {
		return logger;
	}
	
	public NetflowRecordReader(final InputStream in, final RecordSchema schema, ComponentLog logger) {
		// this.reader = new BufferedReader(new InputStreamReader(in));
		this.schema = schema;
		this.logger = logger;

		this.dis = new DataInputStream(new BufferedInputStream(in));
	}

	@Override
	public void close() throws IOException {
		this.dis.close();
	}

	@Override
	public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields)
			throws IOException, MalformedRecordException {
		if (records.isEmpty()) {
			// try and read
			readPacket();
		}
		// look at the queue to see if we have records yet
		return createRecord(records.poll());
	
		// if the queue is empty, parse another packet

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

	public class NetflowRecord extends HashMap<String, Object> implements Map<String, Object> {
		private static final long serialVersionUID = 1L;
	}

	private void readPacket() throws IOException {
		
		int version = dis.readInt();

		int count = -1;
		int uptime = 0;
		int unixtime = 0;
		int sequence = 0;
		int sourceid = 0;

		if (version == 9) {
			count = dis.readShort();
			uptime = dis.readInt();
			unixtime = dis.readInt();
			sequence = dis.readInt();
			sourceid = dis.readInt();
		} else if (version == 10) {
			count = dis.readShort();
			unixtime = dis.readInt();
			sequence = dis.readInt();
			sourceid = dis.readInt();
		}

		for (int i = 0; i < count; i++) {
			short flowsetId = dis.readShort();
			short length = dis.readShort();
			
			byte[] data = new byte[length];
			dis.read(data, 0, length);
			ByteBuffer recordBytes = ByteBuffer.wrap(data);

			if (flowsetId == 0) {
				// if templates, put in cache and maintain a local copy
				NetflowTemplate template = processTemplates(recordBytes);
				NetflowTemplateKey key = new NetflowTemplateKey(String.valueOf(sourceid), template.getId());
				cachePut(key, template);
				getLogger().debug("Received a Flowset Template from %s, with id %d",
						new Object[] { key.getSender(), key.getTemplate() });
			} else if (version == 9 && flowsetId == 1 || version == 10 && flowsetId == 3) {
				// this is an options packet, skip it
				// TODO - implement options packets
				dis.skip(length - 4);
				getLogger().debug("Received an Options Template from %s, with id %d",
						new Object[] { String.valueOf(sourceid) });
			} else if (flowsetId > 255) {
				// if records, check the local then distributed cache for templates
				NetflowTemplateKey key = new NetflowTemplateKey(String.valueOf(sourceid), flowsetId);
				NetflowTemplate template;
				template = cacheGet(key);
				if (template == null) {
					// requeue the packet until the template arrives
					// TODO - handle requeuing to wait for template
					getLogger().error("Received record before template with flowsetId %d", new Object[] { flowsetId });
				} else {
					// it's a record and the have a template for it
					records.add(processFlowRecord(recordBytes, template));
				}
			}
		}
	}

	private NetflowTemplate cacheGet(NetflowTemplateKey key) {
		return this.templates.get(key);
	}

	private void cachePut(NetflowTemplateKey key, NetflowTemplate template) {
		this.templates.put(key, template);
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
		NetflowRecord record = new NetflowRecord();
		for (Field item : template) {
			short length = item.getLength();
			if (length == 65535) {
				// this is a variable length field, which means the first byte is the length of
				// the value
				length = bb.get();
			}
			byte[] dst = new byte[length];
			bb.get(dst, 0, length);
			// TODO - do encoding properly, so it's not just strings
			record.put(item.getName(), new String(dst));
		}
		return record;

	}

	private Record createRecord(NetflowRecord netflowRecord) {
		// read the packet until we have a record
		return new MapRecord(schema, netflowRecord);
	}

	@Override
	public RecordSchema getSchema() throws MalformedRecordException {
		return schema;
	}

}
