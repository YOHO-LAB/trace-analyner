/*
 * Copyright 2015 the original author or authors.
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

package com.yoho.trace.sleuth;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Data transfer object for a collection of spans from a given host.
 *
 * @author Dave Syer
 * @since 1.0.0
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class Spans implements Serializable {

	private Host host;
	private List<Span> spans = Collections.emptyList();
	private long receive ;
	
	@SuppressWarnings("unused")
	private Spans() {
	}

	public Spans(Host host, List<Span> spans) {
		this.host = host;
		this.spans = spans;
	}

	public Host getHost() {
		return this.host;
	}

	public List<Span> getSpans() {
		return this.spans;
	}

	public void setHost(Host host) {
		this.host = host;
	}

	public void setSpans(List<Span> spans) {
		this.spans = spans;
	}

	public long getReceive() {
		return receive;
	}

	public void setReceive(long receive) {
		this.receive = receive;
	}

}
