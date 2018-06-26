package com.yoho.trace.sleuth;

import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.BinaryAnnotation.Type;
import zipkin.Endpoint;
import zipkin.Span.Builder;
import zipkin.collector.Collector;
import zipkin.collector.CollectorSampler;
import zipkin.storage.Callback;
import zipkin.storage.StorageComponent;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * A message listener that is turned on if Sleuth Stream is disabled.
 * Asynchronously stores the received spans using {@link Collector}.
 *
 * @author Dave Syer
 * @since 1.0.0
 */
public class ZipkinMessageListener {

    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(ZipkinMessageListener.class);
    static final String UNKNOWN_PROCESS_ID = "unknown";
    final Collector collector;

    /**
     * lazy so transient storage errors don't crash bootstrap
     */
//	@Lazy
//	@Autowired
    ZipkinMessageListener(StorageComponent storage, CollectorSampler sampler) {
        this.collector = Collector.builder(getClass())
                .storage(storage)
                .sampler(sampler).build();
    }

    public void sink(Spans input) {
        List<zipkin.Span> converted = ConvertToZipkinSpanList.convert(input);
        this.collector.accept(converted, Callback.NOOP);
    }

    /**
     * Add annotations from the sleuth Span.
     */
    static void addZipkinAnnotations(Builder zipkinSpan, Span span, Endpoint endpoint) {
        for (Log ta : span.logs()) {
            Annotation zipkinAnnotation = Annotation.builder()
                    .endpoint(endpoint)
                    .timestamp(ta.getTimestamp() * 1000) // Zipkin is in microseconds
                    .value(ta.getEvent())
                    .build();
            zipkinSpan.addAnnotation(zipkinAnnotation);
        }
    }

    /**
     * Adds binary annotations from the sleuth Span
     */
    static void addZipkinBinaryAnnotations(Builder zipkinSpan, Span span,
                                           Endpoint endpoint) {
        for (Map.Entry<String, String> e : span.tags().entrySet()) {
            BinaryAnnotation.Builder binaryAnn = BinaryAnnotation.builder();
            binaryAnn.type(Type.STRING);
            binaryAnn.key(e.getKey());
            try {
                binaryAnn.value(e.getValue().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                log.error("Error encoding string as UTF-8", ex);
            }
            binaryAnn.endpoint(endpoint);
            zipkinSpan.addBinaryAnnotation(binaryAnn.build());
        }
    }

}
