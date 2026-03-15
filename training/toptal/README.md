# Toptal-style Data Engineering Interview Exercises

Progressive Python exercises for Data Engineering technical interviews.  
Each exercise includes a problem statement, example input/output, constraints, and a function signature. **Solutions are not provided** — solve them yourself.

## Level 1 — Basic Python Data Manipulation

Focus: loops, lists, sets, dictionaries, averages

| # | File | Description |
|---|------|-------------|
| 1 | `1_01_average_per_group.py` | Average per group (user_id, value) |
| 2 | `1_02_remove_duplicates_keep_order.py` | Remove duplicates, preserve order |
| 3 | `1_03_most_frequent_element.py` | Find most frequent element (tie: smallest) |
| 4 | `1_04_sum_of_even_numbers_per_group.py` | Sum of even numbers per user |
| 5 | `1_05_unique_characters_in_string.py` | Unique characters in order of first appearance |
| 6 | `1_06_count_occurrences_of_words.py` | Count occurrences of words |

## Level 2 — Data Transformation

Focus: dictionary joins, grouping, sorting

| # | File | Description |
|---|------|-------------|
| 1 | `2_01_join_two_datasets.py` | Join users and orders, total spending per name |
| 2 | `2_02_sort_users_by_total_score.py` | Aggregate scores, sort descending |
| 3 | `2_03_detect_skewed_keys.py` | Keys representing more than X% of records |

## Level 3 — Top K Problems

Focus: heapq, tuples, streaming thinking

| # | File | Description |
|---|------|-------------|
| 1 | `3_01_top_k_largest_numbers.py` | Top K largest using a heap |
| 2 | `3_02_top_k_frequent_elements.py` | Top K frequent elements |
| 3 | `3_03_top_k_users_by_revenue.py` | Top K users by aggregated revenue |

## Level 4 — Data Engineering Style Challenges

Focus: performance, large datasets, streaming

| # | File | Description |
|---|------|-------------|
| 1 | `4_01_first_non_repeating_event.py` | First element that appears exactly once |
| 2 | `4_02_sliding_window_average.py` | Sliding window average, O(n) |
| 3 | `4_03_duplicate_events_within_time_window.py` | Users with two events within a time window |

## Level 5 — Python built-ins and standard library (interview gaps)

Focus: itertools.groupby, deque, generators (yield), bisect, dict.get/setdefault, min/max(key=), any/all, reduce, zip, sorted multi-criteria, next(iterator, default), string parsing

| # | File | Description |
|---|------|-------------|
| 1 | `5_01_groupby_total_per_date_user.py` | Total amount per (date, user_id) with itertools.groupby |
| 2 | `5_02_sliding_window_with_deque.py` | Sliding window average using deque(maxlen=) |
| 3 | `5_03_csv_chunks_generator.py` | Yield chunks of lines (generator) |
| 4 | `5_04_events_in_time_window_bisect.py` | Count events in [t-T, t] using bisect |
| 5 | `5_05_merge_dicts_sum_values.py` | Merge two dicts, sum values (get/setdefault) |
| 6 | `5_06_min_score_user_with_key.py` | User with minimum score using min(..., key=) |
| 7 | `5_07_all_records_match_rules.py` | All records match all rules (any/all) |
| 8 | `5_08_product_with_reduce.py` | Product of list with functools.reduce |
| 9 | `5_09_merge_dicts_with_reduce.py` | Merge list of dicts (sum per key) with reduce |
| 10 | `5_10_parse_log_line.py` | Parse log line into dict (split/strip) |
| 11 | `5_11_dict_from_zip.py` | Build dict from keys and values with zip() |
| 12 | `5_12_sort_multi_criteria.py` | Sort by score desc, date asc, name asc |
| 13 | `5_13_first_error_event_next.py` | First event with type "ERROR" using next(..., default) |

## How to use

1. Open the exercise file.
2. Read the problem, example, and constraints.
3. Implement the function (signature is given).
4. Test with the example input (print at the end of the file) and run the corner-case test functions (e.g. with pytest or by calling them).
