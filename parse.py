import csv
from collections import OrderedDict
from copy import copy
from dataclasses import dataclass, fields, asdict
import sys
import re
from typing import Callable, Iterator, List


TIMEOUT_VALUE = 300

WITH_TIME = True


def main():
    try:
        test_run = sys.argv[1]
    except:
        print("pass a test run in the cli. for example: python parse.py results0")
        exit(1)
    tests, grouped, grouped_cpu = parse_raw_output(f"{test_run}.txt", WITH_TIME)
    csv_write_dataclasses(ContentionTest, f"{test_run}.csv", tests)
    csv_write_dataclasses(GroupedContentionTest, f"{test_run}_grouped.csv", grouped)
    csv_write_dataclasses(
        GroupedContentionTest, f"{test_run}_grouped_cpu.csv", grouped_cpu
    )
    handle_comprehensive_0_to_10mil_data(grouped, test_run)


def handle_comprehensive_0_to_10mil_data(grouped, test_run):
    aggregate(grouped, REASONABLE, test_run, "reasonable_load", ALL_VARIABLES)
    aggregate(grouped, LIGHT, test_run, "light_load", ALL_VARIABLES)
    aggregate(grouped, HEAVY, test_run, "heavy_load", ALL_VARIABLES)
    aggregate_isolated_writes(grouped, test_run)


def aggregate_isolated_writes(grouped, test_run):
    aggregate(
        grouped,
        LoadProfile(reads_per_second=(0, 1), prior_writes=(0, 11)),
        test_run,
        "no_reads_little_prior",
        ["writes_per_second"],
    )
    aggregate(
        grouped,
        LoadProfile(reads_per_second=(0, 1), prior_writes=(100, 100_001)),
        test_run,
        "no_reads_some_prior",
        ["writes_per_second"],
    )
    aggregate(
        grouped,
        LoadProfile(reads_per_second=(0, 1), prior_writes=(100_000, 10**100)),
        test_run,
        "no_reads_many_prior",
        ["writes_per_second"],
    )


@dataclass
class ContentionTest:
    map_type: str
    shards: int
    prior_writes: int
    writes_per_second: int
    reads_per_second: int
    duration: float
    cpu_time: float


@dataclass
class GroupedContentionTest:
    prior_writes: int
    writes_per_second: int
    reads_per_second: int
    hashmap_duration: float
    dashmap4_duration: float
    dashmap8_duration: float
    # dashmap16_duration: float
    # dashmap32_duration: float
    # dashmap64_duration: float


@dataclass
class AverageContentionTest:
    x: int
    hashmap_duration: float
    dashmap4_duration: float
    dashmap8_duration: float
    # dashmap16_duration: float
    # dashmap32_duration: float
    # dashmap64_duration: float


ALL_VARIABLES = set(["prior_writes", "writes_per_second", "reads_per_second"])


@dataclass
class LoadProfile:
    reads_per_second: tuple = 0, 10**100
    prior_writes: tuple = 0, 10**100
    writes_per_second: tuple = 0, 10**100


# Load that may be encountered for the most popular index entries.
REASONABLE = LoadProfile(
    prior_writes=(1_000, 1_000_001),
    writes_per_second=(1_000, 100_001),
    reads_per_second=(1_000, 1_000_001),
)

# Load that may be typical for some popular entries
# but is likely less than the most popular entries.
LIGHT = LoadProfile(
    prior_writes=(10, 1_001),
    writes_per_second=(1, 1_001),
    reads_per_second=(1, 1_001),
)

# Load that may exceed the typical amount for any index.
HEAVY = LoadProfile(
    prior_writes=(100_000, 10**100),
    writes_per_second=(100_000, 10**100),
    reads_per_second=(100_000, 10**100),
)


def aggregate(
    grouped: List[GroupedContentionTest],
    profile: LoadProfile,
    prefix,
    label: str,
    variables_to_isolate,
):
    for variable_to_group_by in variables_to_isolate:
        variables_to_agg = ALL_VARIABLES - set([variable_to_group_by])
        records = average_by(
            grouped,
            lambda t: getattr(t, variable_to_group_by),
            lambda t: all(
                getattr(t, other) in range(*getattr(profile, other))
                for other in variables_to_agg
            ),
        )
        csv_write_dataclasses(
            AverageContentionTest,
            f"{prefix}.{variable_to_group_by}.{label}.csv",
            records,
            x=variable_to_group_by,
        )


def parse_raw_output(
    path: str, with_time: bool
) -> (List[ContentionTest], List[GroupedContentionTest], List[GroupedContentionTest]):
    tests = []
    with open(path) as f:
        for test in split_tests(f.readlines()):
            try:
                tests.append(parse_test(test, with_time))
            except Exception as e:
                print(f"failed to parse {test}: {e}")
    return (
        tests,
        group_tests(tests, lambda x: x.duration),
        group_tests(tests, lambda x: x.cpu_time),
    )


def split_tests(lines: List[str]) -> Iterator[List[str]]:
    current_test = []
    for line in lines:
        line = line.strip()
        if line == "running load test: Args {":
            if current_test != []:
                yield current_test
            current_test = []
        if line:
            current_test.append(line)
    if current_test != []:
        yield current_test


def parse_test(lines: List[str], with_time: bool) -> ContentionTest:
    cpu_time = None
    duration = None
    if "Contention test duration: " in lines[14]:
        duration = lines[14].split(" ")[-1][:-1]
        if with_time:
            times = re.split("\s+", lines[16].strip())
            real = float(times[0])
            cpu = float(times[2]) + float(times[4])
            cpu_time = float(duration) * cpu / real
    elif "TIMEOUT" in lines[14]:
        duration = TIMEOUT_VALUE
    return ContentionTest(
        map_type=lines[1].split(" ")[1][:-1],
        shards=int(lines[3][0:-1]),
        prior_writes=int(lines[8].split(" ")[-1][:-1]),
        writes_per_second=int(lines[9].split(" ")[-1][:-1]),
        reads_per_second=int(lines[10].split(" ")[-1][:-1]),
        duration=float(duration) if duration is not None else None,
        cpu_time=cpu_time,
    )


def group_tests(
    tests: List[ContentionTest], duration_getter
) -> List[GroupedContentionTest]:
    d = OrderedDict()
    for test in tests:
        key = (test.prior_writes, test.writes_per_second, test.reads_per_second)
        result = d.setdefault(
            key, GroupedContentionTest(*key, None, None, None, None, None, None)
        )
        identity = (test.map_type, test.shards)
        if identity == ("Hashmap", 1):
            result.hashmap_duration = duration_getter(test)
        elif identity == ("Dashmap", 4):
            result.dashmap4_duration = duration_getter(test)
        elif identity == ("Dashmap", 8):
            result.dashmap8_duration = duration_getter(test)
        # elif identity == ("Dashmap", 16):
        #     result.dashmap16_duration = duration_getter(test)
        # elif identity == ("Dashmap", 32):
        #     result.dashmap32_duration = duration_getter(test)
        # elif identity == ("Dashmap", 64):
        #     result.dashmap64_duration = duration_getter(test)
        else:
            print(f"unknown identity: {identity}")
    return [*d.values()]


def csv_write_dataclasses(cls, path: str, records: list, **fieldname_replacements: str):
    fieldnames = [
        fieldname_replacements.get(field.name, field.name) for field in fields(cls)
    ]
    with open(path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for test in records:
            d = asdict(test)
            new_d = {}
            for k, v in d.items():
                new_d[fieldname_replacements.get(k, k)] = v
            writer.writerow(new_d)


def average_by(
    tests: List[GroupedContentionTest],
    field_accessor: Callable[[GroupedContentionTest], int],
    filter: Callable[[GroupedContentionTest], bool],
) -> List[AverageContentionTest]:
    d = OrderedDict()
    for test in tests:
        if filter(test):
            durations = d.setdefault(field_accessor(test), ([], [], []))
            if test.hashmap_duration is not None:
                durations[0].append(test.hashmap_duration)
            if test.dashmap4_duration is not None:
                durations[1].append(test.dashmap4_duration)
            if test.dashmap8_duration is not None:
                durations[2].append(test.dashmap8_duration)
            # if test.dashmap16_duration is not None:
            #     durations[3].append(test.dashmap16_duration)
            # if test.dashmap32_duration is not None:
            #     durations[4].append(test.dashmap32_duration)
            # if test.dashmap64_duration is not None:
            #     durations[5].append(test.dashmap64_duration)
    averaged = []
    for k, v in d.items():
        averaged.append(
            AverageContentionTest(
                k,
                sum(v[0]) / len(v[0]) if len(v[0]) > 0 else None,
                sum(v[1]) / len(v[1]) if len(v[1]) > 0 else None,
                sum(v[2]) / len(v[2]) if len(v[2]) > 0 else None,
                # sum(v[3]) / len(v[3]) if len(v[3]) > 0 else None,
                # sum(v[4]) / len(v[4]) if len(v[4]) > 0 else None,
                # sum(v[5]) / len(v[5]) if len(v[5]) > 0 else None,
            )
        )
    return averaged


if __name__ == "__main__":
    main()
