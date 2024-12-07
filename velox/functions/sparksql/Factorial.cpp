#include "velox/functions/sparksql/Factorial.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions::sparksql {

namespace {

class Factorial : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Ensure the result vector is writable.
    context.ensureWritable(rows, INTEGER(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    // Decode the input vector.
    exec::LocalDecodedVector decodedInput(context, *args[0], rows);

    // Check the mapping type.
    if (decodedInput->isConstantMapping()) {
      handleConstantMapping(rows, decodedInput, flatResult);
    } else if (decodedInput->isIdentityMapping()) {
      handleIdentityMapping(rows, decodedInput, flatResult);
    } else {
      handleDictionaryMapping(rows, decodedInput, flatResult);
    }
  }

 private:
  static constexpr int64_t kFactorials[4] = {1, 1, 2, 6};

  void handleConstantMapping(
      const SelectivityVector& rows,
      exec::DecodedVector* decodedInput,
      FlatVector<int64_t>* flatResult) const {
    if (decodedInput->isNullAt(0)) {
      // All positions are null.
      rows.applyToSelected([&](vector_size_t row) {
        flatResult->setNull(row, true);
      });
    } else {
      // Compute factorial once and set for all rows.
      int64_t input = decodedInput->valueAt<int64_t>(0);
      int64_t factorial = computeFactorial(input);

      if (factorial == -1) {
        // Invalid input; set null.
        rows.applyToSelected([&](vector_size_t row) {
          flatResult->setNull(row, true);
        });
      } else {
        // Valid input; set result.
        rows.applyToSelected([&](vector_size_t row) {
          flatResult->set(row, factorial);
        });
      }
    }
  }

  void handleIdentityMapping(
      const SelectivityVector& rows,
      exec::DecodedVector* decodedInput,
      FlatVector<int64_t>* flatResult) const {
    auto values = decodedInput->data<int64_t>();

    rows.applyToSelected([&](vector_size_t row) {
      if (decodedInput->isNullAt(row)) {
        flatResult->setNull(row, true);
        return;
      }

      int64_t input = values[row];
      int64_t factorial = computeFactorial(input);

      if (factorial == -1) {
        flatResult->setNull(row, true);
      } else {
        flatResult->set(row, factorial);
      }
    });
  }

  void handleDictionaryMapping(
      const SelectivityVector& rows,
      exec::DecodedVector* decodedInput,
      FlatVector<int64_t>* flatResult) const {
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedInput->isNullAt(row)) {
        flatResult->setNull(row, true);
        return;
      }

      int64_t input = decodedInput->valueAt<int64_t>(row);
      int64_t factorial = computeFactorial(input);

      if (factorial == -1) {
        flatResult->setNull(row, true);
      } else {
        flatResult->set(row, factorial);
      }
    });
  }

  int64_t computeFactorial(int64_t input) const {
    if (input >= 0 && input <= 3) {
      return kFactorials[input];
    }
    return -1; // Indicate invalid input.
  }
};

} // namespace

// Registration code omitted for brevity.

} // namespace facebook::velox::functions::sparksql
