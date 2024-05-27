package com.sakame.serializer;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Hessian 序列化器
 *
 * @author sakame
 * @version 1.0
 */
public class HessianSerializer implements Serializer {
    @Override
    public <T> byte[] serialize(T object) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        HessianOutput hessianOutput = new HessianOutput(outputStream);
        hessianOutput.writeObject(object);
        return outputStream.toByteArray();
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        HessianInput hessianInput = new HessianInput(inputStream);
        return (T) hessianInput.readObject(type);
    }
}
