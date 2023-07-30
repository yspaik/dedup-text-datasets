# Copyright 2022 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List, Any, Tuple
import pandas as pd
import os
from collections import defaultdict
import multiprocessing as mp
import numpy as np
import argparse


parser = argparse.ArgumentParser(description="Create Duped info dataset")
parser.add_argument("--data_dir", type=str)
parser.add_argument("--save_dir", type=str)
parser.add_argument("--suffixarray_dir", type=str)
parser.add_argument("--name", type=str)
parser.add_argument("--split", type=str)
parser.add_argument("--remove_dir", type=str)
parser.add_argument('--file_type', type=str, default="csv")
parser.add_argument('--column_name', type=str, default="input")


args = parser.parse_args()


def _dup_points(remove_file: str) -> List:
    dup_points = []  # [[st1, end1], [st2, end2]]
    with open(remove_file) as fin:
        for line in fin:
            if "out" in line:
                break

        for line in fin:
            dup_points.append(list(map(int, line.split())))

    return dup_points


def _decode_string(arr) -> str:
    for x in [
        arr,
        arr[:-1],
        arr[:-2],
        arr[1:],
        arr[1:-1],
        arr[1:-2],
        arr[2:],
        arr[2:-1],
        arr[2:-2],
        arr[3:],
        arr[3:-1],
        arr[3:-2],
        arr[3:-3],
    ]:
        try:
            s = x.decode("utf-8")
            return s
        except UnicodeDecodeError as ude:
            # print(ude)
            continue
    raise UnicodeDecodeError(arr)


# 변환 함수 정의
def convert_to_byte_array(val):
    return np.frombuffer(val.encode(), dtype=np.uint8).tobytes()


dataset = args.name
file_type = args.file_type
column_name = args.column_name
ARRAY_COLUMN_NAME = f"{column_name}_bytearray"


def _check_dup_on_the_cell(params: Tuple) -> Dict[str, Any]:
    this_idx, row = params
    # 중간에 생성된 'input_bytearray' 컬럼 제거 및 'Unnamed: 0' 컬럼 제거
    new_row = row.drop (
        labels=[ARRAY_COLUMN_NAME, 'Unnamed: 0'] if 'Unnamed: 0' in row else [ARRAY_COLUMN_NAME]
    )

    # DataFrame에서 반환받은 row는 Series 타입이므로, to_dict 메서드로 딕셔너리로 변환
    new_row = new_row.to_dict()
    # 인덱스를 딕셔너리에 추가
    new_row["index"] = this_idx

    if this_idx in dup_spans:
        holder = []
        for start, end in dup_spans[this_idx]:
            duped_bytes = row[ARRAY_COLUMN_NAME][start:end]
            holder.append(_decode_string(duped_bytes))
        new_row["dupped_strings"] = str(holder)
        new_row["is_dupped"] = True
    else:
        new_row["dupped_strings"] = ""
        new_row["is_dupped"] = False

    return new_row


remove_suffix = "remove.byterange"
remove_file = os.path.join(args.remove_dir, f"{args.name}.{args.split}.{remove_suffix}")
origin_file = os.path.join(args.data_dir, f"{args.name}.{file_type}")
size_file = os.path.join(args.suffixarray_dir,f"{args.name}.{args.split}.size")
dup_info_file = os.path.join(args.data_dir, f"{args.name}_dup_info.{file_type}")

sizes = np.frombuffer(open(os.path.join(size_file),"rb").read(), dtype=np.uint64)
dup_points = _dup_points(remove_file)
dup_spans = defaultdict(list)  # {0:[(x,y), (x2,y2)], 1:[(a,b), (a2,b2)]}

ptr = 0
for i, byte_start in enumerate(sizes[:-1]):
    byte_end = sizes[i + 1]
    exceed_bounds = False
    # print(byte_start, byte_end, remove[ptr])
    while ptr < len(dup_points) and byte_start <= dup_points[ptr][0] < byte_end:
        # print(remove[ptr])
        # assert dup_points[ptr][1] < byte_end + 6, f"i:{i}, ptr:{ptr}, dup_points:{dup_points[ptr][1]}, byte_end:{byte_end}"
        dup_spans[i].append(
            (
                max(int(dup_points[ptr][0] - byte_start - 6),0) if not exceed_bounds else 0,
                min(int(dup_points[ptr][1] - byte_start), byte_end - byte_start),
            )
        )
        if dup_points[ptr][1] < byte_end + 6:
            exceed_bounds = False
        else:
            exceed_bounds = True
            print("exceed bounds",f"i:{i}, ptr:{ptr}, dup_points:{dup_points[ptr][1]}, byte_end:{byte_end}")
        # The magic value 6의 의미 -> 4-byte index prefix + 2byte suffix(\xff\xff).

        ptr += 1

if file_type == "csv":
    # df = pd.read_csv(origin_file)
    df = pd.read_csv(origin_file, usecols=lambda column: column not in ['Unnamed: 0'])
elif file_type == "xlsx":
    df = pd.read_excel(origin_file)

# apply 함수로 컬럼의 모든 값에 변환 함수 적용
df[ARRAY_COLUMN_NAME] = df[column_name].apply(convert_to_byte_array)

# Apply the processing function
with mp.get_context("fork").Pool(mp.cpu_count()) as p:
    res = p.map(_check_dup_on_the_cell, df.iterrows())
    new_df = pd.DataFrame(res)
    # DataFrame 생성 후에 인덱스를 재설정
    new_df.set_index('index', inplace=True)

# Save to a new CSV
# new_df.to_excel(dup_info_file, index=True)
n_no_dupped = new_df[new_df['is_dupped'] == False].shape[0]
n_dupped = new_df[new_df['is_dupped'] == True].shape[0]
assert new_df.shape[0] == n_dupped + n_no_dupped

new_df.to_csv(dup_info_file, encoding="utf-8-sig")

print(f'{dup_info_file} is created. (shape: {new_df.shape}')
print(f'# dupped rows:{n_dupped}, # not dupped:{n_no_dupped}')
