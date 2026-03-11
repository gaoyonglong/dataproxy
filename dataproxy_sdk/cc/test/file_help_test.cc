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

#include "dataproxy_sdk/file_help.h"

#include <fstream>
#include <iostream>

#include "arrow/builder.h"
#include "gtest/gtest.h"
#include "test/tools/random.h"
#include "test/tools/utils.h"

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

const std::string kCSVFilePath = "test.csv";
const std::string kORCFilePath = "test.orc";
const std::string kBianryFilePath = "test.txt";

template <typename T>
std::unique_ptr<T> GetDefaultFileHelp(const std::string& file_path) {
  auto options = T::Options::Defaults();
  auto ret = T::Make(GetFileFormat(file_path), file_path, options);
  return ret;
}

TEST(FileHelpTest, Binary) {
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("binary_data", arrow::binary())});

  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::Generate(schema, 1);
  auto writer = GetDefaultFileHelp<FileHelpWrite>(kBianryFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kBianryFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, ZeroBinary) {
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("binary_data", arrow::binary())});

  auto binary_builder = arrow::BinaryBuilder();
  CHECK_ARROW_OR_THROW(binary_builder.Append("3\000\00045\0006\000", 8));
  std::shared_ptr<arrow::Array> array;
  ASSIGN_ARROW_OR_THROW(array, binary_builder.Finish());
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.push_back(array);

  std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(schema, arrays.size(), arrays);
  auto writer = GetDefaultFileHelp<FileHelpWrite>(kBianryFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kBianryFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, CSV) {
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kCSVFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, ORC) {
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kORCFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

FileHelpRead::Options GetReadOptions() {
  FileHelpRead::Options read_options = FileHelpRead::Options::Defaults();
  read_options.column_types.emplace("z", arrow::int64());
  read_options.include_columns.push_back("z");
  return read_options;
}

std::vector<int> GetSelectColumns() {
  static std::vector<int> select_columns(1, 2);
  return select_columns;
}

TEST(FileHelpTestWithOption, CSV) {
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFileFormat(kCSVFilePath), kCSVFilePath,
                                   GetReadOptions());
  reader->DoRead(&read_batch);
  reader->DoClose();

  auto target_batch = batch->SelectColumns(GetSelectColumns()).ValueOrDie();
  std::cout << target_batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(target_batch->Equals(*read_batch));
}

TEST(FileHelpTestWithOption, ORC) {
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFileFormat(kORCFilePath), kORCFilePath,
                                   GetReadOptions());
  reader->DoRead(&read_batch);
  reader->DoClose();

  auto target_batch = batch->SelectColumns(GetSelectColumns()).ValueOrDie();
  std::cout << target_batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(target_batch->Equals(*read_batch));
}

FileHelpRead::Options GetErrorOptions() {
  FileHelpRead::Options read_options = FileHelpRead::Options::Defaults();
  read_options.column_types.emplace("a", arrow::int64());
  read_options.include_columns.push_back("a");
  return read_options;
}

TEST(FileHelpTestWithOption, ErrorCSV) {
  auto batch = RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  EXPECT_THROW(FileHelpRead::Make(GetFileFormat(kCSVFilePath), kCSVFilePath,
                                  GetErrorOptions()),
               yacl::Exception);
}

TEST(FileHelpTestWithOption, ErrorORC) {
  auto batch = RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFileFormat(kORCFilePath), kORCFilePath,
                                   GetErrorOptions());
  EXPECT_THROW(reader->DoRead(&read_batch), yacl::Exception);
}

void System(const std::string& cmd) { ASSERT_TRUE(system(cmd.c_str()) == 0); }

TEST(FileHelpTest, LargeBinaryRead) {
  const std::string kLargeBinaryFile = "large_file.txt";
  System("dd if=/dev/urandom of=large_file.txt bs=1M count=1");
  auto reader = GetDefaultFileHelp<FileHelpRead>(kLargeBinaryFile);
  while (1) {
    std::shared_ptr<arrow::RecordBatch> read_batch;
    reader->DoRead(&read_batch);
    if (!read_batch) break;
  }

  reader->DoClose();
}

TEST(FileHelpTest, LargeBinary) {
  System("dd if=/dev/urandom of=large_file_source.txt bs=10M count=1");

  auto writer = GetDefaultFileHelp<FileHelpWrite>("large_file_equal.txt");
  auto reader = GetDefaultFileHelp<FileHelpRead>("large_file_source.txt");
  while (1) {
    std::shared_ptr<arrow::RecordBatch> read_batch;
    reader->DoRead(&read_batch);
    if (!read_batch) break;
    writer->DoWrite(read_batch);
  }
  writer->DoClose();
  reader->DoClose();

  System("diff large_file_source.txt large_file_equal.txt");
}

// Test for CSV null value handling
FileHelpRead::Options GetCSVNullHandlingOptions() {
  FileHelpRead::Options read_options = FileHelpRead::Options::Defaults();
  read_options.column_types.emplace("name", arrow::utf8());
  read_options.include_columns.emplace_back("name");
  read_options.csv_strings_can_be_null = true;
  read_options.csv_null_values = {"NULL", "null", "N/A"};
  return read_options;
}

TEST(FileHelpTestWithOption, CSVNullHandling) {
  // Create a CSV file with null values
  std::ofstream csv_file("test_null.csv");
  csv_file << "name\n";
  csv_file << "Alice\n";
  csv_file << "NULL\n";
  csv_file << "Bob\n";
  csv_file << "null\n";
  csv_file << "N/A\n";
  csv_file << "Charlie\n";
  csv_file.close();

  // Read the CSV file with null value handling
  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(proto::FileFormat::CSV, "test_null.csv",
                                   GetCSVNullHandlingOptions());
  reader->DoRead(&read_batch);
  reader->DoClose();

  // Verify that the null values are properly handled
  ASSERT_NE(read_batch, nullptr);
  ASSERT_EQ(read_batch->num_rows(), 6);

  // Check that the second, fourth, and fifth rows are null
  auto string_array =
      std::dynamic_pointer_cast<arrow::StringArray>(read_batch->column(0));
  ASSERT_NE(string_array, nullptr);

  EXPECT_FALSE(string_array->IsNull(0));  // Alice
  EXPECT_TRUE(string_array->IsNull(1));   // NULL
  EXPECT_FALSE(string_array->IsNull(2));  // Bob
  EXPECT_TRUE(string_array->IsNull(3));   // null
  EXPECT_TRUE(string_array->IsNull(4));   // N/A
  EXPECT_FALSE(string_array->IsNull(5));  // Charlie

  // Clean up
  std::remove("test_null.csv");
}

// Test for CSV null value representation
TEST(FileHelpTestWithOption, CSVNullRepresentation) {
  // Create a RecordBatch with null values
  auto schema = arrow::schema({arrow::field("name", arrow::utf8())});

  arrow::StringBuilder builder;
  CHECK_ARROW_OR_THROW(builder.Append("Alice"));
  CHECK_ARROW_OR_THROW(builder.AppendNull());  // null value
  CHECK_ARROW_OR_THROW(builder.Append("Bob"));
  CHECK_ARROW_OR_THROW(builder.AppendNull());  // null value
  CHECK_ARROW_OR_THROW(builder.Append("Charlie"));

  std::shared_ptr<arrow::Array> array;
  ASSIGN_ARROW_OR_THROW(array, builder.Finish());

  std::vector<std::shared_ptr<arrow::Array>> arrays = {array};
  std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(schema, arrays[0]->length(), arrays);

  // Write to CSV with custom null value representation
  FileHelpWrite::Options write_options = FileHelpWrite::Options::Defaults();
  write_options.csv_null_value = "NULL";  // Custom null value

  auto writer = FileHelpWrite::Make(proto::FileFormat::CSV,
                                    "test_null_output.csv", write_options);
  writer->DoWrite(batch);
  writer->DoClose();

  // Read the CSV file back to verify
  std::ifstream csv_file("test_null_output.csv");
  std::string line;
  std::vector<std::string> lines;

  while (std::getline(csv_file, line)) {
    lines.push_back(line);
  }
  csv_file.close();

  // Verify the content (Arrow CSV writer quotes values by default)
  ASSERT_EQ(lines.size(), 6);       // header + 5 data rows
  EXPECT_EQ(lines[0], "\"name\"");  // header
  EXPECT_EQ(lines[1], "\"Alice\"");
  // With Arrow's native null_string support, null values are written without
  // quotes
  EXPECT_EQ(lines[2],
            "NULL");  // null value represented as NULL (without quotes)
  EXPECT_EQ(lines[3], "\"Bob\"");
  EXPECT_EQ(lines[4],
            "NULL");  // null value represented as NULL (without quotes)
  EXPECT_EQ(lines[5], "\"Charlie\"");

  // Clean up
  std::remove("test_null_output.csv");
}

}  // namespace dataproxy_sdk
