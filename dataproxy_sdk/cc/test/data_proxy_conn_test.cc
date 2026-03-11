// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "dataproxy_sdk/data_proxy_conn.h"

#include <iostream>
#include <thread>

#include "arrow/array/concatenate.h"
#include "arrow/builder.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "gtest/gtest.h"
#include "test/tools/data_mesh_mock.h"
#include "test/tools/random.h"

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

class TestDataProxyConn : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21001"));

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::shared_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
};

class TestDataProxyConnUseDP : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21002", 1));

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::unique_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
};

class TestDataProxyConnChunking : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21003"));
  }

 protected:
  std::unique_ptr<DataMeshMock> data_mesh_;
};

std::shared_ptr<arrow::RecordBatch> DataProxyConnPutAndGet(
    const std::string& ip, const std::shared_ptr<arrow::RecordBatch>& batch) {
  arrow::flight::FlightClientOptions options =
      arrow::flight::FlightClientOptions::Defaults();
  auto dp_conn = DataProxyConn::Connect(ip, false, options);
  auto descriptor = arrow::flight::FlightDescriptor::Command("");

  auto put_result = dp_conn->DoPut(descriptor, batch->schema());
  put_result->WriteRecordBatch(*batch);
  put_result->Close();

  auto get_result = dp_conn->DoGet(descriptor);

  // Read all batches
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (true) {
    auto batch = get_result->ReadRecordBatch();
    if (!batch) break;
    batches.push_back(batch);
  }

  std::shared_ptr<arrow::RecordBatch> result_batch;

  // Combine multiple batches into a single batch
  if (batches.size() > 1) {
    // Combine all batches into a single table
    std::shared_ptr<arrow::Schema> schema = batches[0]->schema();
    std::shared_ptr<arrow::Table> table;
    ASSIGN_ARROW_OR_THROW(table,
                          arrow::Table::FromRecordBatches(schema, batches));

    // Create a single record batch with all the data
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < table->num_columns(); ++i) {
      std::shared_ptr<arrow::ChunkedArray> chunked_array = table->column(i);
      // Concatenate all chunks into a single array
      std::shared_ptr<arrow::Array> concatenated_array;
      ASSIGN_ARROW_OR_THROW(concatenated_array,
                            arrow::Concatenate(chunked_array->chunks()));
      arrays.push_back(concatenated_array);
    }

    result_batch = arrow::RecordBatch::Make(schema, table->num_rows(), arrays);
  } else if (batches.size() == 1) {
    result_batch = batches[0];
  }

  dp_conn->Close();
  return result_batch;
}

TEST_F(TestDataProxyConn, PutAndGet) {
  auto result = DataProxyConnPutAndGet(data_mesh_->GetServerAddress(), data_);

  EXPECT_TRUE(data_->Equals(*result));
}

TEST_F(TestDataProxyConnUseDP, PutAndGet) {
  auto result = DataProxyConnPutAndGet(data_mesh_->GetServerAddress(), data_);

  EXPECT_TRUE(data_->Equals(*result));
}

TEST_F(TestDataProxyConnChunking, PutAndGetLargeBatch) {
  // Create a batch that's guaranteed to exceed the 64MB chunking limit
  auto f0 = arrow::field("id", arrow::int64());
  auto f1 = arrow::field("large_text", arrow::large_utf8());
  auto f2 = arrow::field("value", arrow::int64());
  std::shared_ptr<arrow::Schema> schema = arrow::schema({f0, f1, f2});

  arrow::LargeStringBuilder text_builder;
  arrow::Int64Builder id_builder;
  arrow::Int64Builder value_builder;

  // Create very large strings (10KB each) to quickly reach the 64MB limit
  std::string large_string(10000, 'A');  // 10KB string

  const int num_rows = 10000;  // About 100MB of data

  for (int i = 0; i < num_rows; ++i) {
    CHECK_ARROW_OR_THROW(id_builder.Append(i));
    CHECK_ARROW_OR_THROW(
        text_builder.Append(large_string + "_" + std::to_string(i)));
    CHECK_ARROW_OR_THROW(value_builder.Append(i * 2));
  }

  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> text_array;
  std::shared_ptr<arrow::Array> value_array;
  CHECK_ARROW_OR_THROW(id_builder.Finish(&id_array));
  CHECK_ARROW_OR_THROW(text_builder.Finish(&text_array));
  CHECK_ARROW_OR_THROW(value_builder.Finish(&value_array));

  std::vector<std::shared_ptr<arrow::Array>> arrays = {id_array, text_array,
                                                       value_array};
  auto large_batch = arrow::RecordBatch::Make(schema, num_rows, arrays);

  // Check the size of the batch
  arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();
  int64_t batch_size = 0;
  arrow::Status status =
      arrow::ipc::GetRecordBatchSize(*large_batch, options, &batch_size);

  std::cout << "Batch size: " << batch_size << " bytes ("
            << batch_size / (1024 * 1024) << " MB)" << std::endl;
  EXPECT_TRUE(batch_size / (1024 * 1024) > 64);

  // This batch should be large enough to require chunking
  auto result =
      DataProxyConnPutAndGet(data_mesh_->GetServerAddress(), large_batch);

  // Verify data integrity - the chunking should not affect the data
  EXPECT_TRUE(large_batch->Equals(*result))
      << "Data integrity check failed after chunking";
  std::cout << "SUCCESS: Data integrity maintained after chunking" << std::endl;
}

}  // namespace dataproxy_sdk
