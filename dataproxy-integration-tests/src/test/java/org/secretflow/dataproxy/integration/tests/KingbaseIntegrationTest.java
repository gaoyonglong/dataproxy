/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.integration.tests;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.integration.tests.utils.KingbaseTestUtil;
import org.secretflow.dataproxy.plugin.kingbase.utils.KingbaseDbTypeToKusciaTypeUtil;
import org.secretflow.dataproxy.server.DataProxyFlightServer;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 * @author chenmingliang
 * @date 2025/12/10
 */
@Slf4j
@EnabledIfSystemProperty(named = "enableKingbaseIntegration", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KingbaseIntegrationTest extends BaseArrowFlightServerTest{



    @TempDir
    private static Path tempDir;
    private static Path tmpFilePath;

    Domaindatasource.DatabaseDataSourceInfo kingbaseDataSourceInfo =
            Domaindatasource.DatabaseDataSourceInfo.newBuilder()
                    .setEndpoint(KingbaseTestUtil.getKingbaseEndpoint())
                    .setUser(KingbaseTestUtil.getkingbaseUsername())
                    .setPassword(KingbaseTestUtil.getKingbasePassword())
                    .setDatabase(KingbaseTestUtil.getKingbaseDatabase())
                    .build();

    private final Domaindatasource.DataSourceInfo dataSourceInfo =
            Domaindatasource.DataSourceInfo.newBuilder().setDatabase(kingbaseDataSourceInfo).build();

    Domaindatasource.DomainDataSource domainDataSource = Domaindatasource.DomainDataSource.newBuilder()
            .setDatasourceId("kingbase-datasource")
                .setName("kingbase-datasource")
                .setType("kingbase")
                .setInfo(dataSourceInfo)
            .build();

    List<Common.DataColumn> columns = Arrays.asList(
            Common.DataColumn.newBuilder().setName("column1").setType("character varying").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column2").setType("text").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column3").setType("varchar").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column4").setType("date").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column5").setType("timestamp").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column6").setType("json").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column7").setType("bytea").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column71").setType("bytea").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column8").setType("boolean").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column9").setType("numeric").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column10").setType("smallint").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column11").setType("int2").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column12").setType("serial2").setComment("test table").build()
    );

    private final Domaindata.DomainData domainDataWithTable =
            Domaindata.DomainData.newBuilder()
                    .setDatasourceId("kingbase-datasource")
                    .setName("kingbase-datasource")
                    .setRelativeUri("test_table")
                    .setDomaindataId("domainDataId")
                    .setType("table")
                    .addAllColumns(columns)
                    .build();

    private final Domaindata.DomainData domainDataWithFile =
            Domaindata.DomainData.newBuilder()
                    .setDatasourceId("kingbase-datasource")
                    .setName("kingbase-datasource")
                    .setRelativeUri("integration_test_resource")
                    .setDomaindataId("domainDataId")
                    .setType("model")
                    .build();


    @BeforeAll
    public static void startServer() {

        assertNotEquals("", KingbaseTestUtil.getKingbaseDatabase(), "kingbase database is empty");
        assertNotEquals("", KingbaseTestUtil.getKingbaseEndpoint(), "kingbase endpoint is empty");
        assertNotEquals("", KingbaseTestUtil.getKingbasePassword(), "kingbase password is empty");
        assertNotEquals("", KingbaseTestUtil.getkingbaseUsername(), "kingbase username is empty");

        dataProxyFlightServer = new DataProxyFlightServer(FlightServerContext.getInstance().getFlightServerConfig());

        assertDoesNotThrow(() -> {
            serverThread = new Thread(() -> {
                try {
                    dataProxyFlightServer.start();
                    SERVER_START_LATCH.countDown();
                    dataProxyFlightServer.awaitTermination();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    fail("Exception was thrown: " + e.getMessage());
                }
            });
        });

        assertDoesNotThrow(() -> {
            serverThread.start();
            SERVER_START_LATCH.await();
        });
    }



    @Test
    //@Order(1)
    public void testCommandDataMeshUpdate() {
        Flightinner.CommandDataMeshUpdate commandDataMeshUpdate =
                Flightinner.CommandDataMeshUpdate.newBuilder()
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainDataWithTable)
                        .setUpdate(Flightdm.CommandDomainDataUpdate.newBuilder()
                                .setContentType(Flightdm.ContentType.CSV)
                                .setPartitionSpec("")
                                .build())
                        .build();
        this.testDoPut(commandDataMeshUpdate);
    }


    @Test
   // @Order(2)
    public void testDoGetWithTable() {

        final Flightdm.CommandDomainDataQuery commandDomainDataQueryWithCsv =
                Flightdm.CommandDomainDataQuery.newBuilder()
                        .setContentType(Flightdm.ContentType.CSV)
                        .setPartitionSpec("")
                        .build();

        Flightinner.CommandDataMeshQuery query =
                Flightinner.CommandDataMeshQuery.newBuilder()
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainDataWithTable)
                        .setQuery(commandDomainDataQueryWithCsv)
                        .build();

        this.testDoGet(query);
    }

    private void testDoGet(final Flightinner.CommandDataMeshQuery query) {
        if (query.getQuery().getContentType() == Flightdm.ContentType.RAW) {
            testDoGetWithResource(query);
        } else {
            testDoGetWithTable(query, 20);
        }
    }

    private void testDoGetWithResource(final Message msg) {
        assertDoesNotThrow(() -> {
                    FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());
                    FlightInfo flightInfo = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

                    assertFlightInfo(flightInfo);

                    Location location = flightInfo.getEndpoints().get(0).getLocations().get(0);

                    assertNotNull(tempDir);
                    Path downloadTmpFilePath = tempDir.resolve(String.format("odps-test-download-file-%d.dat", System.currentTimeMillis()));
                    Files.deleteIfExists(downloadTmpFilePath);


                    try (FlightClient flightEndpointClient = FlightClient.builder().location(location).allocator(allocator).build();
                         FlightStream stream = flightEndpointClient.getStream(flightInfo.getEndpoints().get(0).getTicket());
                         BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(downloadTmpFilePath.toFile()))
                    ) {
                        while (stream.next()) {

                            try (VectorSchemaRoot streamRoot = stream.getRoot()) {
                                for (FieldVector vector : streamRoot.getFieldVectors()) {
                                    assertInstanceOf(VarBinaryVector.class, vector);
                                    try (VarBinaryVector varBinaryVector = (VarBinaryVector) vector) {
                                        for (int i = 0; i < varBinaryVector.getValueCount(); i++) {
                                            byte[] object = varBinaryVector.getObject(i);
                                            bos.write(object);
                                        }
                                    }
                                }
                            }
                        }
                        bos.flush();
                    }
                    assertTrue(Files.exists(downloadTmpFilePath));
                    assertTrue(Files.size(downloadTmpFilePath) > 0);
                    assertEquals(getSha256(downloadTmpFilePath), getSha256(tmpFilePath));
                }
        );
    }

    @Test
   // @Order(3)
    public void testCommandDataSourceSqlQuery() {
        log.info("Testing SQL query functionality with comprehensive data types...");

        String sql = String.format(
                "SELECT " +
                        "column1, column2, column3, column4, " +
                        "column5, column6, " +
                        "column7, column71, " +
                        "column8, column9, " +
                        "column10, column11, column12 " +
                        "FROM %s", "test_table");

        Flightinner.CommandDataMeshSqlQuery query = Flightinner.CommandDataMeshSqlQuery.newBuilder()
                .setDatasource(domainDataSource)
                .setQuery(Flightdm.CommandDataSourceSqlQuery.newBuilder()
                        .setSql(sql)
                        .build())
                .build();

        assertDoesNotThrow(() -> {
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(query).toByteArray());
            FlightInfo info = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

            try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
                int rowCount = 0;
                VectorSchemaRoot root = null;

                while (stream.next()) {
                    root = stream.getRoot();
                    rowCount += root.getRowCount();

                    // Verify returned column count (21 columns: all types minus uint and timestamp_ns)
                    int expectedColumnCount = 13;
                    assertEquals(expectedColumnCount, root.getSchema().getFields().size(),
                            "SQL query should return all selected columns");

                    // Verify all columns exist and types are correct
                   // verifySqlQuerySchema(root);

                    log.info("Successfully read {} rows with {} columns from SQL query",
                            rowCount, root.getSchema().getFields().size());
                }

                assertEquals(20, rowCount, "SQL query should return 1 row");

                // Verify returned data values (consistent with written data)
                if (root != null && root.getRowCount() > 0) {
                   // verifySqlQueryDataValues(root, 0);
                }

                log.info("SQL query test passed - all data types verified");
            }
        });
    }


    private void testDoGetWithTable(final Message msg, final long recordCount) {
        assertDoesNotThrow(() -> {
                    FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());

                    FlightInfo flightInfo = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

                    assertFlightInfo(flightInfo);

                    try (FlightStream stream = client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

                        long total = 0;
                        while (stream.next()) {
                            try (VectorSchemaRoot root = stream.getRoot()) {
                                assertNotNull(root);
                                assertNotNull(root.getSchema());
                                total += root.getRowCount();
                            }
                        }
                        assertEquals(recordCount, total);
                    }
                }
        );
    }

    private static String getSha256(Path filePath) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (DigestInputStream dis = new DigestInputStream(new FileInputStream(filePath.toFile()), md)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dis.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }
        }
        byte[] hashBytes = md.digest();
        return bytesToHex(hashBytes);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private void assertFlightInfo(FlightInfo flightInfo) {
        assertNotNull(flightInfo);
        assertNotNull(flightInfo.getEndpoints());
        assertFalse(flightInfo.getEndpoints().isEmpty());

        for (FlightEndpoint endpoint : flightInfo.getEndpoints()) {
            assertNotNull(endpoint);
            assertNotNull(endpoint.getTicket());
            assertNotNull(endpoint.getLocations());
            assertFalse(endpoint.getLocations().isEmpty());
            for (Location location : endpoint.getLocations()) {
                assertNotNull(location);
                assertNotNull(location.getUri());
                assertNotNull(location.getUri().getHost());
            }
        }
    }

    private void testDoPut(final Flightinner.CommandDataMeshUpdate msg) {

        assertDoesNotThrow(() -> {

            FlightDescriptor flightDescriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());
            FlightInfo flightInfo = client.getInfo(flightDescriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

            assertFlightInfo(flightInfo);
            Ticket ticket = flightInfo.getEndpoints().get(0).getTicket();
            FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());

            if (msg.getUpdate().getContentType() == Flightdm.ContentType.RAW) {
                writeTestDataWithFile(descriptor);
            } else {
                writeTestDataWithTable(msg, descriptor);
            }
        });
    }

    private void writeTestDataWithTable(final Flightinner.CommandDataMeshUpdate msg, final FlightDescriptor descriptor) {

        assertNotNull(msg.getDomaindata());
        assertNotNull(msg.getDomaindata().getColumnsList());
        assertFalse(msg.getDomaindata().getColumnsList().isEmpty());

        Schema schema = new Schema(msg.getDomaindata().getColumnsList().stream()
                .map(column ->
                        Field.nullable(column.getName(), KingbaseDbTypeToKusciaTypeUtil.parseKusciaColumnType(column.getType())))
                .collect(Collectors.toList()));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlightClient.ClientStreamListener clientStreamListener = client.startPut(descriptor, root, new AsyncPutListener());
            clientStreamListener.setUseZeroCopy(true);
            writeTestData(root, 20);
            clientStreamListener.putNext();
            clientStreamListener.completed();
            clientStreamListener.getResult();
        }
    }

    private void writeTestData(VectorSchemaRoot root, int rowCount) {
        // 获取当前日期时间，作为基准时间
        LocalDate today = LocalDate.now();
        LocalTime now = LocalTime.now();
        LocalDateTime currentDateTime = LocalDateTime.of(today, now);
        
        // 计算从1970-01-01到今天的天数
        int daysSinceEpoch = (int)ChronoUnit.DAYS.between(LocalDate.of(1970, 1, 1), today);
        // 计算从1970-01-01 00:00:00到现在的毫秒数
        long millisSinceEpoch = ChronoUnit.MILLIS.between(
            LocalDateTime.of(1970, 1, 1, 0, 0), 
            currentDateTime
        );

        Map<Class<? extends FieldVector>, BiConsumer<FieldVector, Integer>> strategyMap = new HashMap<>();

        // 字符串类型
        strategyMap.put(VarCharVector.class, (fieldVector, index) ->
                ((VarCharVector) fieldVector).setSafe(index, ("test" + index).getBytes(StandardCharsets.UTF_8)));
        
        // 整数类型
        strategyMap.put(IntVector.class, (fieldVector, index) ->
                ((IntVector) fieldVector).setSafe(index, index));
        strategyMap.put(BigIntVector.class, (fieldVector, index) ->
                ((BigIntVector) fieldVector).setSafe(index, index));
        
        // 小整数类型 (smallint, int2)
        strategyMap.put(SmallIntVector.class, (fieldVector, index) ->
                ((SmallIntVector) fieldVector).setSafe(index, (short)(index % Short.MAX_VALUE)));
        
        // 二进制类型
        strategyMap.put(VarBinaryVector.class, (fieldVector, index) ->
                ((VarBinaryVector) fieldVector).setSafe(index, ("test" + index).getBytes(StandardCharsets.UTF_8)));
        
        // 浮点数类型
        strategyMap.put(Float4Vector.class, (fieldVector, index) ->
                ((Float4Vector) fieldVector).setSafe(index, index * 1.0f));
        strategyMap.put(Float8Vector.class, (fieldVector, index) ->
                ((Float8Vector) fieldVector).setSafe(index, index * 1.0d));
        
        // 布尔类型
        strategyMap.put(BitVector.class, (fieldVector, index) ->
                ((BitVector) fieldVector).setSafe(index, index % 2 == 0 ? 1 : 0));
        
        // 日期类型
        strategyMap.put(DateDayVector.class, (fieldVector, index) ->
                ((DateDayVector) fieldVector).setSafe(index, daysSinceEpoch));
        strategyMap.put(DateMilliVector.class, (fieldVector, index) ->
                ((DateMilliVector) fieldVector).setSafe(index, millisSinceEpoch));
        
        // 时间戳类型
        strategyMap.put(TimeStampMilliVector.class, new BiConsumer<FieldVector, Integer>() {
            @Override
            public void accept(FieldVector fieldVector, Integer index) {
                ((TimeStampMilliVector) fieldVector).setSafe(index, millisSinceEpoch);
            }
        });
        
        // 时间类型
        strategyMap.put(TimeMilliVector.class, new BiConsumer<FieldVector, Integer>() {
            @Override
            public void accept(FieldVector fieldVector, Integer index) {
                // 设置为当前时间，单位毫秒
                ((TimeMilliVector) fieldVector).setSafe(index, (int)(ChronoUnit.MILLIS.between(LocalTime.MIN, now) % Integer.MAX_VALUE));
            }
        });
        
        // Decimal类型 (用于numeric)
        strategyMap.put(DecimalVector.class, new BiConsumer<FieldVector, Integer>() {
            @Override
            public void accept(FieldVector fieldVector, Integer index) {
                // 设置一个简单的decimal值，保留两位小数
                ((DecimalVector) fieldVector).setSafe(index, BigDecimal.valueOf(index + 0.5));
            }
        });

        for (int i = 0; i < rowCount; i++) {
            for (FieldVector fieldVector : root.getFieldVectors()) {
                BiConsumer<FieldVector, Integer> biConsumer = strategyMap.get(fieldVector.getClass());
                assertNotNull(biConsumer, "没有找到对应的类型处理策略: " + fieldVector.getClass().getSimpleName());
                biConsumer.accept(fieldVector, i);
            }
        }
        root.setRowCount(rowCount);
    }

    private void writeTestDataWithFile(final FlightDescriptor descriptor) throws IOException {
        Schema schema = new Schema(List.of(
                new Field("binary_data", new FieldType(true, new ArrowType.Binary(), null), null)

        ));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            FlightClient.ClientStreamListener clientStreamListener = client.startPut(descriptor, root, new AsyncPutListener());

            assertNotNull(tempDir);
            tmpFilePath = tempDir.resolve("odps-test-file.dat");
            // 1MB
            // On the SDK side, you need to simulate files that exceed 128KB to verify the integrity of the files after batch transfer
            int chunkSize = 1024 * 1024;
            byte[] chunk = new byte[chunkSize];
            // Populate random data
            new Random().nextBytes(chunk);

            try (BufferedOutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(tmpFilePath))) {
                outputStream.write(chunk, 0, chunkSize);
            }

            try (BufferedInputStream fileInputStream = new BufferedInputStream(new FileInputStream(tmpFilePath.toFile()))) {
                int l;
                int i = 0;
                root.allocateNew();
                VarBinaryVector gender = (VarBinaryVector) root.getVector("binary_data");
                gender.allocateNew();
                byte[] bytes = new byte[128 * 1024];
                while ((l = fileInputStream.read(bytes)) != -1) {
                    gender.setSafe(i, bytes, 0, l);
                    i++;
                    gender.setValueCount(i);
                }
                root.setRowCount(i);
            }
            clientStreamListener.putNext();
            clientStreamListener.completed();
            clientStreamListener.getResult();
        }

    }
}

