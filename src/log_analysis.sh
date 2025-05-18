#!/bin/bash

INPUT_LOG="$1"
OUTPUT_PATH="$2"

END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
START_TIME=$(date -d "24 hours ago" '+%Y-%m-%d %H:%M:%S')

if [[ -z "$INPUT_LOG" || -z "$OUTPUT_PATH" ]]; then
    echo "Usage: $0 <log_file> <output_path>"
    exit 1
fi

log_segment=$(awk -v start="$START_TIME" -v end="$END_TIME" '
{
    ts = substr($0, 1, 19)
    if (ts >= start && ts <= end) print $0
}' "$INPUT_LOG")

IFS=$'\n' read -rd '' -a lines <<< "$log_segment"

declare -A row_counts
declare -A success_files
declare -A fail_files
declare -A file_warnings
declare -A seen_success
declare -A seen_fail

success_count=0
fail_count=0
current_file=""

for ((i = 0; i < ${#lines[@]}; i++)); do
    line="${lines[$i]}"

    if [[ "$line" =~ Processing\ file:\ ([^ ]+) ]]; then
        current_file="${BASH_REMATCH[1]}"

    elif [[ "$line" == *"rows =>"* ]]; then
        rows=$(echo "$line" | grep -oE 'rows => [0-9]+' | awk '{print $3}')
        [[ -n "$current_file" && -n "$rows" ]] && row_counts["$current_file"]="$rows"

    elif [[ "$line" =~ Pipeline\ completed\ successfully\ for\ file:\ ([^ ]+) ]]; then
        file="${BASH_REMATCH[1]}"
        if [[ -z "${seen_success[$file]}" ]]; then
            ((success_files["$file"]++))
            ((success_count++))
            seen_success["$file"]=1
        fi

    elif [[ "$line" =~ Pipeline\ failed\ for\ file:\ ([^ ]+) ]]; then
        file="${BASH_REMATCH[1]}"
        if [[ -z "${seen_fail[$file]}" ]]; then
            ((fail_files["$file"]++))
            ((fail_count++))
            seen_fail["$file"]=1
        fi

    elif [[ "$line" =~ \[WARNING\](.*) ]]; then
        file_from_warn=""
        if [[ "$line" =~ on[[:space:]]+([a-zA-Z0-9_]+)\. ]]; then
            file_from_warn="${BASH_REMATCH[1]}"
        fi
        fallback_file="${current_file:-$file_from_warn}"
        [[ -n "$fallback_file" ]] && ((file_warnings["$fallback_file"]++))
    fi
done

{
    echo "=== PIPELINE LOG ANALYSIS ==="
    echo "=== $START_TIME ||| $END_TIME ==="
    echo
    echo "Successful Pipelines: $success_count"
    echo "Failed Pipelines: $fail_count"
    echo

    echo "Success per File:"
    for file in "${!success_files[@]}"; do
        echo "  - $file: ${success_files[$file]}"
    done

    echo
    echo "Warnings per File:"
    for file in "${!file_warnings[@]}"; do
        echo "  - $file: ${file_warnings[$file]}"
    done

    echo
    echo "Failures per File:"
    for file in "${!fail_files[@]}"; do
        echo "  - $file: ${fail_files[$file]}"
    done

    echo
    echo "Row Counts per File:"
    for file in "${!row_counts[@]}"; do
        echo "  - $file: ${row_counts[$file]} rows"
    done
} >> "$OUTPUT_PATH"

echo "✅ Report generated at: $OUTPUT_PATH"
