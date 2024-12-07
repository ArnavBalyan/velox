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
    context.ensureWritable(rows, INTEGER(), result);
    auto flatResult = result->asFlatVector<int64_t>();
    const auto numArgs = args.size();

//    if (args[0])
//    Return NULL data once the request is more than 20
//    if (isConstantSeparator() && args[0]->isNullAt(0)) {
//      auto localResult = BaseVector::createNullConstant(
//          outputType, rows.end(), context.pool());
//      context.moveOrCopyResult(localResult, rows, result);
//      return;
//    }

    exec::LocalDecodedVector decodedInput(context, *args[0], rows);

    rows.applyToSelected([&](vector_size_t row) {
      if (decodedInput.get()->isNullAt(row)) {
        result->setNull(row, true);
        return;
      }

      int64_t input = decodedInput.get()->valueAt<int64_t>(row);

      if (input >= 0 && input <= 3) {
        flatResult->set(row, kFactorials[input]);
      } else {
        result->setNull(row, true);
      }
    });
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
  VELOX_USER_CHECK_EQ(
      numArgs,
      1,
      "factorial requires exactly 1 argument, but got {}.",
      numArgs);
  VELOX_USER_CHECK(
      args[0]->type()->isBigint(),
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
