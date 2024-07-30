# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pytest

from hatchet import GraphFrame


def test_laghos_graphframe(laghos_perfflowaspect_array):
    """Sanity test a GraphFrame object with known data."""
    gf = GraphFrame.from_perfflowaspect_array(str(laghos_perfflowaspect_array),
                                              False, False)

    assert len(gf.dataframe.groupby("name")) == 4

    for col in gf.dataframe.columns:
        if col in ("ts", "dur"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object

    # TODO: add tests to confirm values in dataframe


def test_foobar_graphframe(foobar_perfflowaspect_array):
    """Sanity test a GraphFrame object with known data."""
    gf = GraphFrame.from_perfflowaspect_array(str(foobar_perfflowaspect_array),
                                              False, False)

    assert len(gf.dataframe.groupby("name")) == 3

    for col in gf.dataframe.columns:
        if col in ("ts", "dur"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object

    # TODO: add tests to confirm values in dataframe


def test_ams_mpi_graphframe(ams_mpi_perfflowaspect_array):
    """Sanity test a GraphFrame object with known data."""
    gf = GraphFrame.from_perfflowaspect_array(str(ams_mpi_perfflowaspect_array),
                                              False, False)

    assert len(gf.dataframe.groupby("name")) == 34

    for col in gf.dataframe.columns:
        if col in ("ts", "dur"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object

    # TODO: add tests to confirm values in dataframe


def test_perfflow_detects_no_usage(smoketest_perfflowaspect):
    """Confirm perfflowaspect_reader raises an error when an attempt is made
    to create a graph frame that reads usage statistics, but the supplied
    file does not have any usage statistics."""
    with pytest.raises(ValueError, match="No statistics in the provided file!"):
        gf = GraphFrame.from_perfflowaspect_array(str(smoketest_perfflowaspect),
                                                  True, True)


def test_smoketest_perfflowaspect_array(smoketest_perfflowaspect):
    """Confirm perfflowaspect_reader properly reads a smoketest file.
    There should be no usage statistics in the dataframe.
    """
    gf = GraphFrame.from_perfflowaspect_array(str(smoketest_perfflowaspect),
                                              False, False)

    assert len(gf.dataframe.groupby("name")) == 3

    assert "usage_cpu" not in gf.dataframe.columns
    assert "usage_memory" not in gf.dataframe.columns


def test_perfflowaspectobjectreader(perfflowaspectobjectreader_test_file):
    gf = GraphFrame.from_perfflowaspect_object(str(perfflowaspectobjectreader_test_file))

    assert len(gf.dataframe.groupby("name")) == 3

    for col in gf.dataframe.columns:
        if col in ("ts", "dur"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object


def test_smoketest_perfflowaspect_stats_memory(smoketest_perfflowaspect_stats):
    """Confirm perfflowaspect_reader reads only memory in a smoketest example
    with statistics There should be no cpu statistics if successful.
    """
    gf = GraphFrame.from_perfflowaspect_array(str(smoketest_perfflowaspect_stats),
                                              True, False)

    assert len(gf.dataframe.groupby("name")) == 3

    assert all(column in gf.dataframe.columns for column in (
        "ts", "dur", "usage_memory", "pid", "name", "ph"
    ))

    assert "usage_cpu" not in gf.dataframe.columns

    for col in gf.dataframe.columns:
        if col in ("ts", "dur"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid", "usage_memory"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object


def test_smoketest_perfflowaspect_stats_cpu(smoketest_perfflowaspect_stats):
    """Confirm perfflowaspect_reader reads only cpu in a smoketest example
    with statistics There should be no memory statistics if successful.
    """
    gf = GraphFrame.from_perfflowaspect_array(str(smoketest_perfflowaspect_stats),
                                              False, True)

    assert len(gf.dataframe.groupby("name")) == 3

    assert all(column in gf.dataframe.columns for column in (
        "ts", "dur", "usage_cpu", "pid", "name", "ph"
    ))

    assert "usage_memory" not in gf.dataframe.columns

    for col in gf.dataframe.columns:
        if col in ("ts", "dur", "usage_cpu"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid",):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object


def test_smoketest_perfflowaspect_stats(smoketest_perfflowaspect_stats):
    """Confirm perfflowaspect_reader reads both usage statistics in a
    smoketest example with statistics. There should be cpu/memory stats.
    """
    gf = GraphFrame.from_perfflowaspect_array(str(smoketest_perfflowaspect_stats),
                                              True, True)

    assert len(gf.dataframe.groupby("name")) == 3

    assert all(column in gf.dataframe.columns for column in (
        "ts", "dur", "usage_cpu", "usage_memory", "pid", "name", "ph"
    ))

    for col in gf.dataframe.columns:
        if col in ("ts", "dur", "usage_cpu"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid", "usage_memory"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object


def test_smoketest_two_perfflowaspect_stats(smoketest_two_perfflowaspect_stats):
    """Confirm perfflowaspect_reader reads both usage statistics in a
    smoketest2 example with statistics. There should be cpu/memory stats.
    """
    gf = GraphFrame.from_perfflowaspect_array(str(smoketest_two_perfflowaspect_stats),
                                              True, True)

    assert len(gf.dataframe.groupby("name")) == 1

    assert all(column in gf.dataframe.columns for column in (
        "ts", "dur", "usage_cpu", "usage_memory", "pid", "name", "ph"
    ))

    for col in gf.dataframe.columns:
        if col in ("ts", "dur", "usage_cpu"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid", "usage_memory"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object


def test_smoketest_three_perfflowaspect(smoketest_three_perfflowaspect):
    """Confirm perfflowaspect_reader reads both usage statistics in a
    smoketest2 example with statistics. There should be cpu/memory stats.
    """
    gf = GraphFrame.from_perfflowaspect_array(str(smoketest_three_perfflowaspect),
                                              False, False)

    assert len(gf.dataframe.groupby("name")) == 3

    assert all(column in gf.dataframe.columns for column in (
        "ts", "dur",  "pid", "name", "ph"
    ))

    assert "usage_cpu" not in gf.dataframe.columns
    assert "usage_memory" not in gf.dataframe.columns

    for col in gf.dataframe.columns:
        if col in ("ts", "dur", "usage_cpu"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid", "usage_memory"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object


def test_smoketest_three_perfflowaspect_stats(smoketest_three_perfflowaspect_stats):
    """Confirm perfflowaspect_reader reads both usage statistics in a
    smoketest2 example with statistics. There should be cpu/memory stats.
    """
    gf = GraphFrame.from_perfflowaspect_array(str(smoketest_three_perfflowaspect_stats),
                                              True, True)

    assert len(gf.dataframe.groupby("name")) == 3

    assert all(column in gf.dataframe.columns for column in (
        "ts", "dur", "usage_cpu", "usage_memory", "pid", "name", "ph"
    ))

    for col in gf.dataframe.columns:
        if col in ("ts", "dur", "usage_cpu"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid", "usage_memory"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object


def test_perfflowaspectobjectreader_timestamp_conversion(perfflowaspectobjectreader_test_file):
    gf = GraphFrame.from_perfflowaspect_object(str(perfflowaspectobjectreader_test_file))

    def us_to_s(microseconds):
        return microseconds / 1e6

    gf.dataframe['ts_s'] = gf.dataframe['ts'].apply(us_to_s)
    gf.dataframe['dur_s'] = gf.dataframe['dur'].apply(us_to_s)

    for _, row in gf.dataframe.iterrows():
        assert row['ts_s'] == row['ts'] / 1e6
        assert row['dur_s'] == row['dur'] / 1e6

    print(gf.dataframe[['ts', 'ts_s', 'dur', 'dur_s']])