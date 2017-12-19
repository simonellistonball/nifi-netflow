package com.simonellistonball.nifi.processors.netflow;

import java.util.Map;

import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;

public class NetflowEventFactory implements EventFactory<NetflowEvent> {

	private final DistributedMapCacheClient templateCache;

	public NetflowEventFactory(DistributedMapCacheClient templateCache) {
		super();
		this.templateCache = templateCache;
	}

	@Override
	public NetflowEvent create(byte[] data, Map<String, String> metadata, ChannelResponder responder) {
		
		// what have we got
		
		// if templates, put in cache and maintain a local copy
		
		// if records, check the local then distributed cache for templates
		
		// use template to output fields
		
		templateCache.get(key, keySerializer, valueDeserializer)
	}

}
