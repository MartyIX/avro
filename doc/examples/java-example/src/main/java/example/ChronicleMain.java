package example;

import example.avro.User;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

// rm -rf data && mvn compile &&  mvn -e -q exec:java -Dexec.mainClass=example.ChronicleMain
public class ChronicleMain {

    public static void main(String[] args) throws IOException {

        AvroHelper<User> helper = new AvroHelper<>(5000, User.class);

        // Write
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder("data").build()) {
            ExcerptAppender appender = queue.acquireAppender();

            int i = 0;
            for (String color : new String[]{"red", "green", "yellow"}) {
                i++;
                User user = new User("User_" + i, 7, color);

                appender.writeBytes(b -> {
                    EncoderByteArrayOS eba = helper.serialize(user);
                    b.writeInt(eba.getCount());
                    b.write(eba.getBuf(), 0, eba.getCount());
                });
            }
        }

        // Read
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder("data").build()) {
            ExcerptTailer tailer = queue.createTailer();

            User reusable = new User();

            while (true) {
                boolean read = tailer.readBytes(b -> {
                    int len = b.readInt();
                    b.read(helper.getClearDecoderByteBuffer().array(), 0, len);
                });

                if (!read) break;

                User user = helper.deserialize(reusable);
                System.out.println(user.toString());
            }
        }
    }

    public static class AvroHelper<T> {

        private final BinaryEncoder binaryEncoder;
        private final EncoderByteArrayOS encoderOutputStream;
        @SuppressWarnings("unused")
        private final BinaryDecoder binaryDecoder;
        public final ByteBuffer decoderByteBuffer;
        private final DatumReader<T> datumReader;
        private DatumWriter<T> datumWriter;

        public AvroHelper(int bufferSize, Class<T> tClass) {
            this.datumWriter = new SpecificDatumWriter<>(tClass);
            this.encoderOutputStream = new EncoderByteArrayOS(bufferSize);
            this.binaryEncoder = EncoderFactory.get().directBinaryEncoder(encoderOutputStream, null);

            this.datumReader = new SpecificDatumReader<>(tClass);
            byte[] decoderBytes = new byte[bufferSize];
            this.decoderByteBuffer = ByteBuffer.wrap(decoderBytes);
            this.binaryDecoder = DecoderFactory.get().binaryDecoder(decoderBytes, null);
        }

        public EncoderByteArrayOS serialize(T datum) {
            encoderOutputStream.reset();

            try {
                datumWriter.write(datum, binaryEncoder);
                binaryEncoder.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return encoderOutputStream;
        }

        public T deserialize(@Nullable T reusableDatum) throws IOException {
            decoderByteBuffer.clear();
            // @todo: Use binaryDecoder field here
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(decoderByteBuffer.array(), null);
            return datumReader.read(reusableDatum, binaryDecoder);
        }

        public ByteBuffer getClearDecoderByteBuffer() {
            decoderByteBuffer.clear();
            return decoderByteBuffer;
        }
    }

    public static class EncoderByteArrayOS extends ByteArrayOutputStream {

        public EncoderByteArrayOS(int size) {
            super(size);
        }

        public int getCount() {
            return count;
        }

        public byte[] getBuf() {
            return buf;
        }
    }
}