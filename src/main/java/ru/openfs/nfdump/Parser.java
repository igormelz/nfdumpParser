package ru.openfs.nfdump;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.*;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.camel.Exchange;
import org.apache.camel.ExtendedExchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.SynchronizationAdapter;
import org.apache.camel.util.FileUtil;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.tuples.Tuple5;
import io.vertx.core.file.OpenOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.core.file.AsyncFile;
import ru.openfs.nfdump.model.FlowRecord;

@ApplicationScoped
public class Parser extends RouteBuilder {
    final static String outSessionHeader = "subnet,last,proto,asn,local_ip,local_port,remote_ip,remote_port,ul,dl,bytes_in,bytes_out\n";
    // private final static int FIRST = 1;
    // private final static int FIRSTMS = 2;
    private final static int LAST = 3;
    // private final static int LASTMS = 4;
    private final static int PROTO = 5;
    private final static int SRCIP = 9;
    private final static int SRCPORT = 10;
    private final static int DSTIP = 14;
    private final static int DSTPORT = 15;
    private final static int SRCAS = 16;
    private final static int DSTAS = 17;
    private final static int INIF = 18;
    private final static int OUTIF = 19;
    private final static int BYTES = 23;

    @ConfigProperty(name = "uplinks", defaultValue = "100,101")
    Set<String> uplinks;

    @ConfigProperty(name = "localASN", defaultValue = "111111")
    int localAsn;

    @ConfigProperty(name = "srcdir", defaultValue = "in")
    String srcdir;

    @ConfigProperty(name = "dstdir", defaultValue = "out")
    String dstdir;

    @Inject
    IpNetworks nets;

    @Inject
    Vertx vertx;

    @Override
    public void configure() throws Exception {
        from("file:" + srcdir + "?delete=true&sortBy=file:name")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        Log.infof("Setting next file to: %s", exchange.getIn().getHeader("CamelFileName", "defaultValue"));
                        File output = createTempFile(dstdir);
                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(exchange.getIn().getMandatoryBody(InputStream.class),
                                        Charset.forName("UTF-8")))) {
                            long start = System.currentTimeMillis();
                            AsyncFile outFile = vertx.fileSystem().openAndAwait(output.getAbsolutePath(),
                                    new OpenOptions());
                            // write header
                            outFile.writeAndAwait(Buffer.buffer(outSessionHeader));
                            // process records
                            reader.lines().parallel()
                                    .map(line -> line.split("\\|"))
                                    .filter(f -> f.length > BYTES)
                                    .filter(f -> uplinks.contains(f[INIF]) || uplinks.contains(f[OUTIF]))
                                    .map(f -> createRecord(f))
                                    .filter(fr -> fr.getSubnet() != null)
                                    .filter(fr -> fr.getAsn() != localAsn)
                                    .collect(
                                            groupingByConcurrent(
                                                    fr -> Tuple5.of(fr.getLocalIp(),
                                                            fr.getLocalPort(),
                                                            fr.getRemoteIp(),
                                                            fr.getRemotePort(),
                                                            fr.getProto()),
                                                    reducing((fr1, fr2) -> fr1.appendAndGet(fr2))))
                                    .values().stream().parallel()
                                    .filter(fr -> fr.isPresent() && fr.get().getUplink() != 0
                                            && fr.get().getDownlink() != 0)
                                    .map(Parser::outSession)
                                    .forEach(fr -> outFile.write(Buffer.buffer(fr)).subscribe()
                                            .with(ok -> {
                                            }, fail -> Log.error(fail)));

                            Log.infof("Processed at %d ms", (System.currentTimeMillis() - start));
                        } catch (IOException x) {
                            Log.error(x);
                        }
                        exchange.getIn().setBody(output, File.class);
                        exchange.adapt(ExtendedExchange.class).addOnCompletion(new CleanupTempFile(output));
                    }
                })
                .to("file:" + dstdir + "?fileName=${file:name.noext}.csv");
    }

    private class CleanupTempFile extends SynchronizationAdapter {
        private File cleanupFile;

        public CleanupTempFile(File fileToCleanup) {
            cleanupFile = fileToCleanup;
        }

        @Override
        public void onDone(Exchange exchange) {
            FileUtil.deleteFile(this.cleanupFile);
        }
    }

    FlowRecord createRecord(String[] f) {
        if (uplinks.contains(f[INIF])) {
            return new FlowRecord(
                    Long.valueOf(f[LAST]),
                    nets.find(Integer.parseUnsignedInt(f[DSTIP])),
                    Integer.valueOf(f[PROTO]),
                    Integer.valueOf(f[SRCAS]),
                    Integer.parseUnsignedInt(f[DSTIP]),
                    Integer.valueOf(f[DSTPORT]),
                    Integer.parseUnsignedInt(f[SRCIP]),
                    Integer.valueOf(f[SRCPORT]),
                    Long.valueOf(f[BYTES]), 0,
                    Integer.valueOf(f[INIF]), 0);
        }
        return new FlowRecord(
                Long.valueOf(f[LAST]),
                nets.find(Integer.parseUnsignedInt(f[SRCIP])),
                Integer.valueOf(f[PROTO]),
                Integer.valueOf(f[DSTAS]),
                Integer.parseUnsignedInt(f[SRCIP]),
                Integer.valueOf(f[SRCPORT]),
                Integer.parseUnsignedInt(f[DSTIP]),
                Integer.valueOf(f[DSTPORT]),
                0, Long.valueOf(f[BYTES]),
                0, Integer.valueOf(f[OUTIF]));
    }

    public final static String outSession(Optional<FlowRecord> opt) {
        FlowRecord record = opt.get();
        StringBuffer out = new StringBuffer(record.getSubnet()).append(',')
                .append(record.getLast()).append(',')
                .append(record.getProto()).append(',')
                .append(record.getAsn()).append(',')
                .append(ntoa(record.getLocalIp())).append(',')
                .append(record.getLocalPort()).append(',')
                .append(ntoa(record.getRemoteIp())).append(',')
                .append(record.getRemotePort()).append(',')
                .append(record.getUplink()).append(',')
                .append(record.getDownlink()).append(',')
                .append(record.getBytesIn()).append(',')
                .append(record.getBytesOut()).append('\n');
        return out.toString();
    }

    static File createTempFile(String dstdir) throws IOException {
        return FileUtil.createTempFile("camel", ".tmp",
                new File(FileUtil.normalizePath(dstdir)));
    }

    public final static String ntoa(int ip) {
        return ((ip >> 24) & 0xFF) + "." +
                ((ip >> 16) & 0xFF) + "." +
                ((ip >> 8) & 0xFF) + "." +
                (ip & 0xFF);
    }
}
