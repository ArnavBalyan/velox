/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/functions/sparksql/Factorial.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/VectorFunction.h"
#include <iostream>

namespace facebook::velox::functions::sparksql {

namespace {
class Factorial : public exec::VectorFunction {
 public:
  Factorial() = default;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Log the number of arguments passed
    const auto numArgs = args.size();
    std::cout << "[FactorialFunction] Number of arguments: " << numArgs << std::endl;
    for (size_t i = 0; i < args.size(); ++i) {
      std::cout << "[FactorialFunction] Argument " << i << ": Type = " << args[i]->type()->toString() << std::endl;
    }

    for (size_t i = 0; i < args.size(); ++i) {
      std::cout << "Argument " << i << ": Type = " << args[i]->type()->toString() << std::endl;
      if (args[i]->isConstantEncoding()) {
        std::cout << "Argument " << i << " is a constant vector with value: "
                  << args[i]->as<ConstantVector<int64_t>>()->valueAt(0) << std::endl;
      } else if (args[i]->isFlatEncoding()) {
        std::cout << "Argument " << i << " is a flat vector." << std::endl;
      } else if (args[i]->isDictionaryEncoding()) {
        std::cout << "Argument " << i << " is a dictionary vector." << std::endl;
      }
    }

    // Ensure the result vector is writable
    context.ensureWritable(rows, BIGINT(), result);
    std::cout << "[FactorialFunction] Result vector initialized and writable." << std::endl;

    auto* flatResult = result->asFlatVector<int64_t>();

    // Decode the input vector
    exec::DecodedArgs decodedArgs(rows, args, context);
    std::cout << "[FactorialFunction] Input vector decoding initialized." << std::endl;

    auto* inputVector = decodedArgs.at(0);

    // Log the number of selected rows
    std::cout << "[FactorialFunction] Number of rows to process: " << rows.end() << std::endl;

    // Process each row
    rows.applyToSelected([&](vector_size_t row) {
      if (inputVector->isNullAt(row)) {
        std::cout << "[FactorialFunction] Row " << row << " is null, setting result to null." << std::endl;
        flatResult->setNull(row, true);
      } else {
        int64_t value = inputVector->valueAt<int64_t>(row);
        std::cout << "[FactorialFunction] Row " << row << ": Input value = " << value << std::endl;

        int64_t factorial = 0;
        std::cout << "[FactorialFunction] Row " << row << ": Computed factorial = " << factorial << std::endl;

        flatResult->set(row, factorial);
      }
    });

    std::cout << "[FactorialFunction] Completed processing all rows." << std::endl;
  }

 private:
  static constexpr int64_t kFactorials[4] = {1, 1, 2, 6};
};
} // namespace

TypePtr FactorialCallToSpecialForm::resolveType(
    const std::vector<TypePtr>&) {
  return INTEGER();
}

exec::ExprPtr FactorialCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  auto numArgs = args.size();

  std::cout << "Number of arguments: " << numArgs << std::endl;
  for (size_t i = 0; i < args.size(); ++i) {
    std::cout << "Argument " << i << ": Type = " << args[i]->type()->toString() << std::endl;
  }

  VELOX_USER_CHECK_EQ(
      numArgs,
      1,
      "factorial requires exactly 1 argument, but got {}.",
      numArgs);
  VELOX_USER_CHECK(
      args[0]->type()->isInteger(),
      "The argument of factorial must be an integer.");

  auto factorial = std::make_shared<Factorial>();
  return std::make_shared<exec::Expr>(
      type,
      std::move(args),
      std::move(factorial),
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
      "factorial",
      trackCpuUsage);
}
} // namespace facebook::velox::functions::sparksql
