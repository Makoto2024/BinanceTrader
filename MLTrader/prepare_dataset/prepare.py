import pandas as pd
import numpy as np

import dataclasses
import datetime


SPLIT_PER_DAY: int = 23
TRAINING_DAY: int = 20
VALIDATION_DAY: int = SPLIT_PER_DAY - TRAINING_DAY

BAR_PER_DATAPOINT: int = 2 * 24 * (60 // 15)  # 2 days of 5 minute bars.
OUTPUT_BAR_PER_DATAPOINT: int = 4 * (60 // 15)  # 4 hours of 5 minute bars.
INPUT_BAR_PER_DATAPOINT: int = BAR_PER_DATAPOINT - OUTPUT_BAR_PER_DATAPOINT

DIMENSION_PER_BAR: int = 4  # (open, close, high, low)


@dataclasses.dataclass(frozen=True)
class Dataset:
    X: np.array  # Input data of the model. [N x INPUT_BAR_PER_DATAPOINT X DIMENSION_PER_BAR]
    Y: np.array  # Output data of the model. [N X OUTPUT_DIMENSION]

    def __add__(self, other):
        if not isinstance(other, Dataset):
            return NotImplemented
        return Dataset(
            X=np.concatenate((self.X, other.X), axis=0),
            Y=np.concatenate((self.Y, other.Y), axis=0)
        )


def empty_dataset() -> Dataset:
    return Dataset(
        X=np.empty((0, INPUT_BAR_PER_DATAPOINT, DIMENSION_PER_BAR), dtype=np.float64),
        Y=np.empty((0, OUTPUT_BAR_PER_DATAPOINT, DIMENSION_PER_BAR), dtype=np.float64)
    )


def ms_timestamp_to_datetime(ms: pd.Timestamp) -> datetime.datetime:
    return ms.to_pydatetime()


def extract_open_close_high_low(df: pd.DataFrame) -> np.array:
    return df.values[:, 2:6].reshape((1, -1, DIMENSION_PER_BAR)).astype(np.float64)


def sliding_window_sample_datapoints(df: pd.DataFrame) -> Dataset:
    if len(df) < BAR_PER_DATAPOINT:  # No enough data for one sliding window.
        return []
    dataset: Dataset = empty_dataset()
    for start_idx in range(0, len(df) - BAR_PER_DATAPOINT + 1, 3):
        data_point = df.iloc[start_idx:start_idx+BAR_PER_DATAPOINT]
        input_data = extract_open_close_high_low(data_point.iloc[:INPUT_BAR_PER_DATAPOINT])
        output_data = extract_open_close_high_low(data_point.iloc[INPUT_BAR_PER_DATAPOINT:])
        dataset = dataset + Dataset(X=input_data, Y=output_data)
    return dataset

def split_train_validation(csv_path: str) -> tuple[Dataset, Dataset]:
    df = pd.read_csv(csv_path)

    # Convert timestamps to datetime objects
    df["OpenTime"] = pd.to_datetime(df["OpenTime"], unit="ms")
    df["CloseTime"] = pd.to_datetime(df["CloseTime"], unit="ms")

    training_data: Dataset = empty_dataset()
    validation_data: Dataset = empty_dataset()

    start_date = ms_timestamp_to_datetime(df["OpenTime"].min())
    end_date = ms_timestamp_to_datetime(df["CloseTime"].max())
    current_date = start_date

    while current_date + pd.Timedelta(days=SPLIT_PER_DAY) <= end_date:
        print(f"Processing date {current_date} ~ {current_date + pd.Timedelta(days=SPLIT_PER_DAY)}")
        # For each SPLIT_PER_DAY days, the first TRAINING_DAY days are for training data,
        # the last VALIDATION_DAY days are for validation data.
        training_start = current_date
        training_end = current_date + pd.Timedelta(days=TRAINING_DAY)
        validation_start = current_date + pd.Timedelta(days=TRAINING_DAY)
        validation_end = current_date + pd.Timedelta(days=SPLIT_PER_DAY)

        training_df = df[(df["OpenTime"] >= training_start) & (df["CloseTime"] < training_end)]
        validation_df = df[(df["OpenTime"] >= validation_start) & (df["CloseTime"] < validation_end)]

        # Sample training and validation data using sliding window.
        training_data = training_data + sliding_window_sample_datapoints(training_df)
        validation_data = validation_data + sliding_window_sample_datapoints(validation_df)

        # Move to next SPLIT_PER_DAY.
        current_date += pd.Timedelta(days=SPLIT_PER_DAY)

    # Convert to numpy arrays
    return (training_data, validation_data)


if __name__ == "__main__":
    symbol = "SOLUSDT"
    csv_path = f"../../price_data/{symbol}/{symbol}_5m.csv"
    training_data, validation_data = split_train_validation(csv_path)

    print("Data preparation complete. Files saved as .npy files.")
    print(np.shape(training_data.X))
    print(np.shape(training_data.Y))
    print(np.shape(validation_data.X))
    print(np.shape(validation_data.Y))
    np.save("train_X.npy", training_data.X, allow_pickle=False)
    np.save("train_Y.npy", training_data.Y, allow_pickle=False)
    np.save("validation_X.npy", validation_data.X, allow_pickle=False)
    np.save("validation_Y.npy", validation_data.Y, allow_pickle=False)
