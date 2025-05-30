/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Operator/PartitionColumnFillingTransform.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(TestPartitionColumnFillingTransform, TestInt32)
{
    auto int_type = DataTypeFactory::instance().get("Int32");
    auto column0 = int_type->createColumn();
    column0->insert(1);
    column0->insert(2);
    column0->insert(3);
    column0->insert(4);

    ColumnsWithTypeAndName input_columns = {ColumnWithTypeAndName(int_type, "colA")};
    Block input(input_columns);
    ColumnsWithTypeAndName output_columns = {ColumnWithTypeAndName(int_type, "colB"), ColumnWithTypeAndName(int_type, "colA")};
    Block output(output_columns);
    String partition_name = "colB";
    String partition_value = "8";
    auto transformer = local_engine::PartitionColumnFillingTransform(input, output, partition_name, partition_value);

    Chunk chunk;
    chunk.addColumn(std::move(column0));
    transformer.transform(chunk);
    ASSERT_EQ(2, chunk.getNumColumns());
    WhichDataType which(chunk.getColumns().at(0)->getDataType());
    ASSERT_TRUE(which.isInt32());
}


TEST(TestPartitionColumnFillingTransform, TestFloat32)
{
    auto int_type = DataTypeFactory::instance().get("Int32");
    auto float32_type = DataTypeFactory::instance().get("Float32");

    auto column0 = int_type->createColumn();
    column0->insert(1);
    column0->insert(2);
    column0->insert(3);
    column0->insert(4);

    ColumnsWithTypeAndName input_columns = {ColumnWithTypeAndName(int_type, "colA")};
    Block input(input_columns);
    ColumnsWithTypeAndName output_columns = {ColumnWithTypeAndName(int_type, "colA"), ColumnWithTypeAndName(float32_type, "colB")};
    Block output(output_columns);
    String partition_name = "colB";
    String partition_value = "3.1415926";
    auto transformer = local_engine::PartitionColumnFillingTransform(input, output, partition_name, partition_value);

    Chunk chunk;
    chunk.addColumn(std::move(column0));
    transformer.transform(chunk);
    ASSERT_EQ(2, chunk.getNumColumns());
    WhichDataType which(chunk.getColumns().at(1)->getDataType());
    ASSERT_TRUE(which.isFloat32());
}

TEST(TestPartitionColumnFillingTransform, TestDate)
{
    auto int_type = DataTypeFactory::instance().get("Int32");
    auto date_type = DataTypeFactory::instance().get("Date");

    auto column0 = int_type->createColumn();
    column0->insert(1);
    column0->insert(2);
    column0->insert(3);
    column0->insert(4);

    ColumnsWithTypeAndName input_columns = {ColumnWithTypeAndName(int_type, "colA")};
    Block input(input_columns);
    ColumnsWithTypeAndName output_columns = {ColumnWithTypeAndName(int_type, "colA"), ColumnWithTypeAndName(date_type, "colB")};
    Block output(output_columns);
    String partition_name = "colB";
    String partition_value = "2022-01-01";
    auto transformer = local_engine::PartitionColumnFillingTransform(input, output, partition_name, partition_value);

    Chunk chunk;
    chunk.addColumn(std::move(column0));
    transformer.transform(chunk);
    ASSERT_EQ(2, chunk.getNumColumns());
    WhichDataType which(chunk.getColumns().at(1)->getDataType());
    ASSERT_TRUE(which.isUInt16());
}

TEST(TestPartitionColumnFillingTransform, TestString)
{
    auto int_type = DataTypeFactory::instance().get("Int32");
    auto string_type = DataTypeFactory::instance().get("String");

    auto column0 = int_type->createColumn();
    column0->insert(1);
    column0->insert(2);
    column0->insert(3);
    column0->insert(4);

    ColumnsWithTypeAndName input_columns = {ColumnWithTypeAndName(int_type, "colA")};
    Block input(input_columns);
    ColumnsWithTypeAndName output_columns = {ColumnWithTypeAndName(int_type, "colA"), ColumnWithTypeAndName(string_type, "colB")};
    Block output(output_columns);
    String partition_name = "colB";
    String partition_value = "2022-01-01";
    auto transformer = local_engine::PartitionColumnFillingTransform(input, output, partition_name, partition_value);

    Chunk chunk;
    chunk.addColumn(std::move(column0));
    transformer.transform(chunk);
    ASSERT_EQ(2, chunk.getNumColumns());
    WhichDataType which(chunk.getColumns().at(1)->getDataType());
    ASSERT_TRUE(which.isString());
}
