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

#pragma once
#include <Core/Block.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{
/// This step will return empty block.
class EarlyStopStep : public DB::ITransformingStep
{
public:
    explicit EarlyStopStep(const DB::SharedHeader & input_header_);
    ~EarlyStopStep() override = default;

    String getName() const override { return "EarlyStopStep"; }

    static DB::Block transformHeader(const DB::Block & input);

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &) override;

    void describeActions(DB::IQueryPlanStep::FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;
};

class EarlyStopTransform : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    explicit EarlyStopTransform(const DB::SharedHeader & header_);
    ~EarlyStopTransform() override = default;

    Status prepare() override;
    void work() override;
    String getName() const override { return "EarlyStopTransform"; }

private:
    DB::Block header;
};
}
