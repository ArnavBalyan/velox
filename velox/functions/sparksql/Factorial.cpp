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
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/functions/sparksql/Factorial.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/DecodedVector.h"
#include "velox/functions/Registerer.h"
#include <iostream>

namespace facebook::velox::functions::sparksql {

namespace {

class Factorial : public exec::VectorFunction {
 public:
  Factorial() = default;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Ensure the result vector is writable and of the correct type.
    context.ensureWritable(rows, BIGINT(), result);
    auto flatResult = result->asFlatVector<int64_t>();
    const auto numArgs = args.size();

    std::cout << "Number of arguments: " << numArgs << std::endl;

    // Decode the input vector.
    DecodedVector decodedInput(*args[0], rows);
    std::cout << "Decoded input vector initialized." << std::endl;

    // Check if the input vector is a constant mapping.
    if (decodedInput.isConstantMapping()) {
      std::cout << "Input is constant mapping." << std::endl;

      if (decodedInput.isNullAt(0)) {
        std::cout << "Input is a constant null." << std::endl;
        // Set all result positions to null.
        rows.applyToSelected([&](vector_size_t row) {
          flatResult->setNull(row, true);
        });
      } else {
        // Get the constant input value.
        int64_t input = decodedInput.valueAt<int64_t>(0);
        std::cout << "Input constant value: " << input << std::endl;

        // Compute the factorial.
        int64_t factorial = computeFactorial(input);

        if (factorial == -1) {
          std::cout << "Input value out of range. Setting nulls." << std::endl;
          // Set all result positions to null.
          rows.applyToSelected([&](vector_size_t row) {
            flatResult->setNull(row, true);
          });
        } else {
          // Set all result positions to the factorial value.
          rows.applyToSelected([&](vector_size_t row) {
            flatResult->set(row, factorial);
          });
        }
      }
    } else {
      std::cout << "Input is not constant mapping." << std::endl;

      // Process each selected row.
      rows.applyToSelected([&](vector_size_t row) {
        std::cout << "Processing row: " << row << std::endl;

        if (decodedInput.isNullAt(row)) {
          std::cout << "Row " << row << " is null in input." << std::endl;
          flatResult->setNull(row, true);
          return;
        }

        // Get the input value for the row.
        int64_t input = decodedInput.valueAt<int64_t>(row);
        std::cout << "Row " << row << ", input value: " << input << std::endl;

        // Compute the factorial.
        int64_t factorial = computeFactorial(input);

        if (factorial == -1) {
          std::cout << "Row " << row << " input out of range. Setting null." << std::endl;
          flatResult->setNull(row, true);
        } else {
          std::cout << "Row " << row << ", factorial value: " << factorial << std::endl;
          flatResult->set(row, factorial);
        }
      });
    }
    std::cout << "Completed processing rows." << std::endl;
  }

 private:
  static constexpr int64_t kFactorials[4] = {1, 1, 2, 6};

  // Helper function to compute the factorial for valid inputs.
  int64_t computeFactorial(int64_t input) const {
    if (input >= 0 && input <= 3) {
      return kFactorials[input];
    }
    return -1; // Indicate invalid input.
  }
};

} // namespace

// Function to register the 'factorial' function.
void registerFactorial(const std::string& name) {
  // Define the function signature: bigint -> bigint.
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build(),
  };

  // Register the vector function with the given name.
  exec::registerVectorFunction(
      name,
      std::move(signatures),
      std::make_unique<Factorial>());
}

} // namespace facebook::velox::functions::sparksql
