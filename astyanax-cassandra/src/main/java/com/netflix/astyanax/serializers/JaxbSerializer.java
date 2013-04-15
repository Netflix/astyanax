package com.netflix.astyanax.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import com.netflix.astyanax.connectionpool.exceptions.SerializationException;

/**
 * Serializes Objects using Jaxb. An instance of this class may only serialize
 * JAXB compatible objects of classes known to its configured context.
 * 
 * @author shuzhang0@gmail.com
 */
public class JaxbSerializer extends AbstractSerializer<Object> {

    /** The cached per-thread marshaller. */
    private ThreadLocal<Marshaller> marshaller;
    /** The cached per-thread unmarshaller. */
    private ThreadLocal<Unmarshaller> unmarshaller;

    /**
     * Lazily initialized singleton factory for producing default
     * XMLStreamWriters.
     */
    private static XMLOutputFactory outputFactory;
    /**
     * Lazily initialized singleton factory for producing default
     * XMLStreamReaders.
     */
    private static XMLInputFactory inputFactory;

    /**
     * Constructor.
     * 
     * @param serializableClasses
     *            List of classes which can be serialized by this instance. Note
     *            that concrete classes directly referenced by any class in the
     *            list will also be serializable through this instance.
     */
    public JaxbSerializer(final Class... serializableClasses) {
        marshaller = new ThreadLocal<Marshaller>() {
            @Override
            protected Marshaller initialValue() {
                try {
                    return JAXBContext.newInstance(serializableClasses).createMarshaller();
                }
                catch (JAXBException e) {
                    throw new IllegalArgumentException("Classes to serialize are not JAXB compatible.", e);
                }
            }
        };

        unmarshaller = new ThreadLocal<Unmarshaller>() {
            @Override
            protected Unmarshaller initialValue() {
                try {
                    return JAXBContext.newInstance(serializableClasses).createUnmarshaller();
                }
                catch (JAXBException e) {
                    throw new IllegalArgumentException("Classes to serialize are not JAXB compatible.", e);
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer toByteBuffer(Object obj) {
        if (obj == null) {
            return null;
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            XMLStreamWriter writer = createStreamWriter(buffer);
            marshaller.get().marshal(obj, writer);
            writer.flush();
            writer.close();
        }
        catch (JAXBException e) {
            throw new SerializationException("Object to serialize " + obj
                    + " does not seem compatible with the configured JaxbContext;"
                    + " note this Serializer works only with JAXBable objects.", e);
        }
        catch (XMLStreamException e) {
            throw new SerializationException("Exception occurred writing XML stream.", e);
        }
        return ByteBuffer.wrap(buffer.toByteArray());
    }

    /** {@inheritDoc} */
    @Override
    public Object fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null || !bytes.hasRemaining()) {
            return null;
        }
        ByteBuffer dup = bytes.duplicate();
        ByteArrayInputStream bais = new ByteArrayInputStream(dup.array());
        try {
            XMLStreamReader reader = createStreamReader(bais);
            Object ret = unmarshaller.get().unmarshal(reader);
            reader.close();
            return ret;
        }
        catch (JAXBException e) {
            throw new SerializationException("Jaxb exception occurred during deserialization.", e);
        }
        catch (XMLStreamException e) {
            throw new SerializationException("Exception reading XML stream.", e);
        }
    }

    /**
     * Get a new XML stream writer.
     * 
     * @param output
     *            An underlying OutputStream to write to.
     * @return a new {@link XMLStreamWriter} which writes to the specified
     *         OutputStream. The output written by this XMLStreamWriter is
     *         understandable by XMLStreamReaders produced by
     *         {@link #createStreamReader(InputStream)}.
     * @throws XMLStreamException
     */
    // Provides hook for subclasses to override how marshalling results are
    // serialized (ex. encoding).
    protected XMLStreamWriter createStreamWriter(OutputStream output) throws XMLStreamException {
        if (outputFactory == null) {
            outputFactory = XMLOutputFactory.newInstance();
        }
        return outputFactory.createXMLStreamWriter(output);
    }

    /**
     * Get a new XML stream reader.
     * 
     * @param input
     *            the underlying InputStream to read from.
     * @return a new {@link XmlStreamReader} which reads from the specified
     *         InputStream. The reader can read anything written by
     *         {@link #createStreamWriter(OutputStream)}.
     * @throws XMLStreamException
     */
    protected XMLStreamReader createStreamReader(InputStream input) throws XMLStreamException {
        if (inputFactory == null) {
            inputFactory = XMLInputFactory.newInstance();
        }
        return inputFactory.createXMLStreamReader(input);
    }

}
