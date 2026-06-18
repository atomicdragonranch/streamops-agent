package com.streamops.processor.functions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Kryo serializer that delegates to Protobuf's own binary format instead of
 * Kryo's default field-by-field reflection. Protobuf's internal collections
 * (UnmodifiableCollection in MapField, etc.) are not compatible with Kryo's
 * CollectionSerializer.
 */
public class ProtobufKryoSerializer<T extends GeneratedMessageV3> extends Serializer<T> {

    @Override
    public void write(Kryo kryo, Output output, T message) {
        byte[] bytes = message.toByteArray();
        output.writeVarInt(bytes.length, true);
        output.writeBytes(bytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(Kryo kryo, Input input, Class<? extends T> type) {
        int length = input.readVarInt(true);
        byte[] bytes = input.readBytes(length);
        try {
            Method parseFrom = type.getMethod("parseFrom", byte[].class);
            return (T) parseFrom.invoke(null, bytes);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof InvalidProtocolBufferException) {
                throw new RuntimeException("Failed to parse protobuf message", e.getCause());
            }
            throw new RuntimeException(e);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Protobuf class missing parseFrom(byte[]): " + type.getName(), e);
        }
    }
}
