# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pytest
import os

from hatchet import GraphFrame


def test_laghos_graphframe(laghos_perfflowaspect_array):
    """Sanity test a GraphFrame object with known data."""
    gf = GraphFrame.from_perfflowaspect(str(laghos_perfflowaspect_array))

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
    gf = GraphFrame.from_perfflowaspect(str(foobar_perfflowaspect_array))

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
    gf = GraphFrame.from_perfflowaspect(str(ams_mpi_perfflowaspect_array))

    assert len(gf.dataframe.groupby("name")) == 34

    for col in gf.dataframe.columns:
        if col in ("ts", "dur"):
            assert gf.dataframe[col].dtype == np.float64
        elif col in ("pid", "tid"):
            assert gf.dataframe[col].dtype == np.int64
        elif col in ("name", "ph"):
            assert gf.dataframe[col].dtype == object

    # TODO: add tests to confirm values in dataframe
    

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