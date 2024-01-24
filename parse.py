import csv
from collections import OrderedDict
from copy import copy
from dataclasses import dataclass, fields, asdict
import sys
from typing import Callable, Iterator, List


TIMEOUT_VALUE = 300


class Reasonable:
    """Load that may be encountered for the most popular index entries."""

    PRIOR_WRITES = range(1_000, 1_000_001)
    WRITES_PER_SECOND = range(1_000, 100_001)
    READS_PER_SECOND = range(1_000, 1_000_001)


class Light:
    """Load that may be typical for some popular entries
    but is likely less than the most popular entries."""

    PRIOR_WRITES = range(10, 1_001)
    WRITES_PER_SECOND = range(1, 1_001)
    READS_PER_SECOND = range(1, 1_001)


class Heavy:
    """Load that may exceed the typical amount for any index."""

    PRIOR_WRITES = range(100_000, 10**100)
    WRITES_PER_SECOND = range(100_000, 10**100)
    READS_PER_SECOND = range(100_000, 10**100)


def main():
    try:
        test_run = sys.argv[1]
    except:
        print("pass a test run in the cli. for example: python parse.py results0")
        exit(1)
    tests, grouped = parse_raw_output(f"{test_run}.txt")
    csv_write_dataclasses(ContentionTest, f"{test_run}.csv", tests)
    csv_write_dataclasses(GroupedContentionTest, f"{test_run}_grouped.csv", grouped)
    aggregate(grouped, Reasonable, test_run, "reasonable_load", [])
    aggregate(grouped, Light, test_run, "light_load", [])
    aggregate(grouped, Heavy, test_run, "heavy_load", [])


@dataclass
class ContentionTest:
    map_type: str
    shards: int
    prior_writes: int
    writes_per_second: int
    reads_per_second: int
    duration: float


@dataclass
class GroupedContentionTest:
    prior_writes: int
    writes_per_second: int
    reads_per_second: int
    hashmap_duration: float
    dashmap4_duration: float
    dashmap8_duration: float


@dataclass
class AverageContentionTest:
    x: int
    hashmap_duration: float
    dashmap4_duration: float
    dashmap8_duration: float


VARIABLES = set(f.name for f in fields(GroupedContentionTest)) - set(
    f.name for f in fields(AverageContentionTest)
)


def aggregate(
    grouped: List[GroupedContentionTest], filters, prefix, label: str, exclude
):
    for var in VARIABLES:
        if var not in exclude:
            others = VARIABLES - set([var])
            records = average_by(
                grouped,
                lambda t: getattr(t, var),
                lambda t: all(
                    getattr(t, other) in getattr(filters, other.upper())
                    for other in others
                ),
            )
            csv_write_dataclasses(
                AverageContentionTest,
                f"{prefix}.{var}.{label}.csv",
                records,
                x=var,
            )


def parse_raw_output(path: str) -> (List[ContentionTest], List[GroupedContentionTest]):
    tests = []
    with open(path) as f:
        for test in split_tests(f.readlines()):
            try:
                tests.append(parse_test(test))
            except Exception as e:
                print(f"failed to parse {test}: {e}")
    return tests, group_tests(tests)


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


def parse_test(lines: List[str]) -> ContentionTest:
    if "Contention test duration: " in lines[14]:
        duration = lines[14].split(" ")[-1][:-1]
    elif "TIMEOUT" in lines[14]:
        duration = TIMEOUT_VALUE
    return ContentionTest(
        map_type=lines[1].split(" ")[1][:-1],
        shards=int(lines[3][0]),
        prior_writes=int(lines[8].split(" ")[-1][:-1]),
        writes_per_second=int(lines[9].split(" ")[-1][:-1]),
        reads_per_second=int(lines[10].split(" ")[-1][:-1]),
        duration=float(duration) if duration is not None else None,
    )


def group_tests(tests: List[ContentionTest]) -> List[GroupedContentionTest]:
    d = OrderedDict()
    for test in tests:
        key = (test.prior_writes, test.writes_per_second, test.reads_per_second)
        result = d.setdefault(key, GroupedContentionTest(*key, None, None, None))
        identity = (test.map_type, test.shards)
        if identity == ("Hashmap", 1):
            result.hashmap_duration = test.duration
        elif identity == ("Dashmap", 4):
            result.dashmap4_duration = test.duration
        elif identity == ("Dashmap", 8):
            result.dashmap8_duration = test.duration
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
    averaged = []
    for k, v in d.items():
        averaged.append(
            AverageContentionTest(
                k,
                sum(v[0]) / len(v[0]) if len(v[0]) > 0 else None,
                sum(v[1]) / len(v[1]) if len(v[1]) > 0 else None,
                sum(v[2]) / len(v[2]) if len(v[2]) > 0 else None,
            )
        )
    return averaged


if __name__ == "__main__":
    main()
