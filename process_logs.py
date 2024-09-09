import re
import sys


def read_log_file(file_path):
    with open(file_path, encoding="utf-8") as file:
        return file.readlines()


def filter_logs(log_lines):
    exclude_phrases = [
        "Start eligibility test",
        "Start scheduling the routine",
        "New coming routine",
        "pass the eligibility test",
        "Executing step call service",
        "Start measurement",
        "Max polls",
        "# polls",
    ]
    filtered_logs = [
        line
        for line in log_lines
        if line.strip() and not any(phrase in line for phrase in exclude_phrases)
    ]
    return filtered_logs


def replace_dynamic_text(log_lines):
    pattern = r"<built-in method cancel of _asyncio\.Task object at 0x[0-9a-fA-F]+>"
    new_text = "timer object"
    return [re.sub(pattern, new_text, line) for line in log_lines]


def create_routine_aliases(filtered_logs):
    routine_aliases = {}
    routine_counter = 0
    routine_triggers = []

    for line in filtered_logs:
        if "Scheduling routine" in line:
            routine_match = re.search(r"routine (\S+) took (\d+\.\d+) seconds", line)
            if routine_match:
                routine_id = routine_match.group(1)
                trigger_seconds = float(routine_match.group(2))
                routine_triggers.append((trigger_seconds, routine_id))

    # Sort routines by trigger seconds
    # routine_triggers.sort()

    for _, routine_id in routine_triggers:
        if routine_id not in routine_aliases:
            routine_aliases[routine_id] = f"R{routine_counter}"
            routine_counter += 1

    return routine_aliases


def replace_routine_aliases(logs: list[str], routine_aliases: dict[str, str]):
    # Replace routine IDs and action IDs in all messages
    replaced_logs = []
    for line in logs:
        replaced_log = line
        for routine_id, alias in routine_aliases.items():
            replaced_log = re.sub(routine_id, alias, replaced_log)
            replaced_log = re.sub(routine_id + r"(\.\d+)", alias + r"\1", replaced_log)
        replaced_logs.append(replaced_log)

    return replaced_logs


def write_logs_to_file(log_lines, output_file_path):
    with open(output_file_path, "w", encoding="utf-8") as file:
        for line in log_lines:
            file.write(line)


def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print(
            "Usage: python process_logs.py <log_file_path> <output_file_path> [--include-before-start | -i]"
        )
        sys.exit(1)

    log_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    include_before_start = len(sys.argv) == 4 and (
        sys.argv[3] == "--include-before-start" or sys.argv[3] == "-i"
    )

    log_lines = read_log_file(log_file_path)

    if not include_before_start:
        # Find the index of the "Starting Home Assistant" line
        start_index = next(
            (
                i
                for i, line in enumerate(log_lines)
                if "Starting Home Assistant" in line
            ),
            None,
        )
        if start_index is not None:
            log_lines = log_lines[start_index:]

    filtered_logs = filter_logs(log_lines)
    replaced_logs = replace_dynamic_text(filtered_logs)
    routine_aliases = create_routine_aliases(replaced_logs)
    replaced_logs = replace_routine_aliases(replaced_logs, routine_aliases)
    routine_aliases_str = "\n".join(
        f"{alias}: {routine_id}" for routine_id, alias in routine_aliases.items()
    )
    replaced_logs.insert(0, routine_aliases_str)

    write_logs_to_file(replaced_logs, output_file_path)


if __name__ == "__main__":
    main()
