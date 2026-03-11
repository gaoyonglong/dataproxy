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

#include "utils.h"

#include <filesystem>
#include <fstream>
#include <unordered_map>

#include "arrow/ipc/writer.h"
#include "spdlog/spdlog.h"

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

std::string ReadFileContent(const std::string& file) {
  if (!std::filesystem::exists(file)) {
    DATAPROXY_ENFORCE("can not find file: {}", file);
  }
  std::ifstream file_is(file);
  DATAPROXY_ENFORCE(file_is.good(), "open failed, file: {}", file);
  return std::string((std::istreambuf_iterator<char>(file_is)),
                     std::istreambuf_iterator<char>());
}

std::shared_ptr<arrow::DataType> GetDataType(const std::string& type) {
  static std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
      type_map = {{"int8", arrow::int8()},
                  {"int16", arrow::int16()},
                  {"int32", arrow::int32()},
                  {"int64", arrow::int64()},
                  {"uint8", arrow::uint8()},
                  {"uint16", arrow::uint16()},
                  {"uint32", arrow::uint32()},
                  {"uint64", arrow::uint64()},
                  {"float16", arrow::float16()},
                  {"float32", arrow::float32()},
                  {"float64", arrow::float64()},
                  {"bool", arrow::boolean()},
                  {"int", arrow::int64()},
                  {"float", arrow::float64()},
                  {"str", arrow::utf8()},
                  {"string", arrow::utf8()},
                  {"large_str", arrow::large_utf8()},
                  {"large_string", arrow::large_utf8()},
                  {"large_utf8", arrow::large_utf8()}};

  auto iter = type_map.find(type);
  if (iter == type_map.end()) {
    DATAPROXY_THROW("Unsupported type: {}", type);
  }

  return iter->second;
}

int64_t GetBatchSize(const arrow::RecordBatch& batch) {
  int64_t size = 0;
  arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();
  arrow::Status status = arrow::ipc::GetRecordBatchSize(batch, options, &size);
  if (!status.ok()) {
    SPDLOG_WARN("Failed to calculate batch size: {}", status.ToString());
    return -1;
  }
  return size;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> SplitBatch(
    const arrow::RecordBatch& batch, int64_t max_chunk_size) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> chunks;
  int64_t batch_size = GetBatchSize(batch);
  if (batch_size <= 0 || batch_size <= max_chunk_size) {
    // No need to split - create a shared_ptr with no-op deleter
    auto shared_batch = std::shared_ptr<arrow::RecordBatch>(
        const_cast<arrow::RecordBatch*>(&batch), [](arrow::RecordBatch*) {});
    chunks.push_back(shared_batch);
    return chunks;
  }
  int64_t num_rows = batch.num_rows();
  int64_t chunk_rows =
      std::max(int64_t(1), num_rows * max_chunk_size / batch_size);
  for (int64_t offset = 0; offset < num_rows; offset += chunk_rows) {
    int64_t length = std::min(chunk_rows, num_rows - offset);
    // Create a shared_ptr with no-op deleter for slicing
    auto shared_batch = std::shared_ptr<arrow::RecordBatch>(
        const_cast<arrow::RecordBatch*>(&batch), [](arrow::RecordBatch*) {});
    std::shared_ptr<arrow::RecordBatch> chunk =
        shared_batch->Slice(offset, length);
    chunks.push_back(chunk);
  }
  SPDLOG_INFO("Split batch of size {} bytes and {} rows into {} chunks",
              batch_size, num_rows, chunks.size());
  return chunks;
}

}  // namespace dataproxy_sdk