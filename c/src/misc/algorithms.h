#pragma once

#include <vector>
#include <functional>
#include <stdint.h>
#include <numeric>

namespace {
    template <typename T, typename GetValueCallback>
    void _find_shortest_combination(const std::vector<T>& values, int target, std::vector<T>& best_combo, uint32_t values_offset, std::vector<T> resulting_set, GetValueCallback get_value) {
        for (uint32_t i = values_offset; i < values.size(); i++) {
            int current_sum = (int)get_value(values[i]);

            // Do not iterate if the target is above what we expect.
            // This can also be used to return the maximum value of an integer to avoid including it in the sum!
            if (current_sum > target) continue;
            
            // Calculate the total we have so far.
            for (const T& n : resulting_set) current_sum += get_value(n);

            // If the total will be above our target, we're not interested in exploring that branch.
            // Do not remove this unless you want each operation to take 5 minutes, time complexity is O(2^n*n)!
            if (current_sum > target) continue;

            // Add a new value to the running total.
            resulting_set.reserve(values.size()); // List of combinations will never grow above the length of the possible values.
            resulting_set.push_back(values[i]);

            // Some rules to determine whether to replace the best combination.
            // Update the best combo if either there isn't already one or the potential candidate is closer to the target with less values.
            if (best_combo.empty()) best_combo = resulting_set;
            else {
                // Get the sum of the best combo.
                int best_combo_sum = 0;
                for (const T& n : best_combo) best_combo_sum += get_value(n);

                if (best_combo_sum != target) {
                    if (current_sum == target) best_combo = resulting_set;
                    else if (current_sum > best_combo_sum) best_combo = resulting_set;
                    else if (current_sum >= best_combo_sum && resulting_set.size() < best_combo.size()) best_combo = resulting_set;
                } else if (current_sum == target && resulting_set.size() < best_combo.size()) best_combo = resulting_set;
            }

            // Jump into the next possible branch.
            _find_shortest_combination(values, target, best_combo, i + 1, std::move(resulting_set), get_value);
        }
    }
}

namespace algorithms {
    template <typename T, typename GetValueCallback>
    std::vector<T> find_shortest_combination(const std::vector<T>& values, int target, GetValueCallback get_value) {
        std::vector<T> best_combo;

        _find_shortest_combination(values, target, best_combo, 0, std::vector<T>(), get_value);
        return best_combo;
    }
}