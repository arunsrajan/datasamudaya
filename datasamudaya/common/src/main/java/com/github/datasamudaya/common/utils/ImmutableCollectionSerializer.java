package com.github.datasamudaya.common.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableCollection;

public class ImmutableCollectionSerializer extends Serializer {
	@Override
    public void write(Kryo kryo, Output output, Object object) {
        ImmutableCollection<?> immutableCollection = (ImmutableCollection<?>) object;
        kryo.writeClassAndObject(output, immutableCollection.stream().collect(Collectors.toCollection(ArrayList::new)));
        output.flush();
    }

    @Override
    public Collection read(Kryo kryo, Input input, Class type) {
    	return (Collection) kryo.readObject(input, type);
    }
}

