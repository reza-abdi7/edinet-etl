from unittest.mock import patch

import pandas as pd
import pytest

from src.load import load_to_csv


@pytest.fixture
def sample_df():
    data = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']}
    return pd.DataFrame(data)


@pytest.fixture
def empty_df():
    return pd.DataFrame()


def test_load_to_csv_creates_directory_and_saves_file(tmp_path, sample_df):
    output_dir = tmp_path / 'some_subfolder'
    file_name = 'test_file.csv'
    full_path = output_dir / file_name

    load_to_csv(sample_df, str(output_dir), str(file_name))

    assert full_path.exists(), 'Expected CSV file to be created.'

    saved_df = pd.read_csv(full_path)
    pd.testing.assert_frame_equal(sample_df, saved_df, check_dtype=False)


def test_load_to_csv_with_empty_df(tmp_path, empty_df, caplog):
    output_dir = tmp_path / 'test'
    file_name = 'empty.csv'
    full_path = output_dir / file_name

    load_to_csv(empty_df, str(output_dir), str(file_name))

    assert not full_path.exists(), 'No file should be created for an empty DataFrame.'

    assert any(
        'Attempted to load an empty DataFrame' in message
        for message in caplog.text.splitlines()
    ), 'Expected a warning in the log about empty DataFrame.'


def test_load_to_csv_exception_handling(tmp_path, sample_df, caplog):
    output_dir = tmp_path / 'output'
    file_name = 'error.csv'

    with patch.object(
        pd.DataFrame, 'to_csv', side_effect=OSError('Simulated write error')
    ):
        load_to_csv(sample_df, str(output_dir), str(file_name))

    assert any(
        'Error while writing CSV' in message for message in caplog.text.splitlines()
    ), 'Expected an error message in the logs due to simulated write failure.'
