#!/bin/bash

TFDS_DIR=tmp/data/hf_challenge/
DATA_DIR=tmp/saved/hf_challenge/
TMP_DIR=tmp/
DATASET=prep_orca_gpt4

FILE_TYPE=csv
COLLUMN_NAME=question

SPLIT=train
REMOVE_FILE=remove.byterange
CLICHE=_dup
THRESHOLD=100

CACHE=tmp/cache/

# Start time
start=$(date +%s)

cargo build

python3 scripts/load_dataset.py --data_dir $TFDS_DIR --save_dir $DATA_DIR --name $DATASET --split $SPLIT --file_type $FILE_TYPE --column_name $COLLUMN_NAME
#  loading time
inter_a=$(date +%s)
diff_a=$(( inter_a - start ))
echo "Loading Time elapsed: $diff_a seconds."


python3 scripts/make_suffix_array.py $DATA_DIR$DATASET.$SPLIT
#  make suffix time from start
inter_b=$(date +%s)
diff_b=$(( inter_b - inter_a ))
echo "Making Suffix Time elapsed: $diff_b seconds."

cargo run self-similar --data-file $DATA_DIR$DATASET.$SPLIT --length-threshold $THRESHOLD --cache-dir $CACHE

cargo run collect --data-file $DATA_DIR$DATASET.$SPLIT --length-threshold $THRESHOLD --cache-dir $CACHE > tmp/$DATASET.$SPLIT.remove.byterange
# collect time from start
inter_c=$(date +%s)
diff_c=$(( inter_c - inter_b ))
echo "Collect Time elapsed: $diff_c seconds."

python3 scripts/finish_with_dup_info.py --data_dir $TFDS_DIR --save_dir $DATA_DIR --name $DATASET --file_type $FILE_TYPE --column_name $COLLUMN_NAME --split $SPLIT --suffixarray_dir $DATA_DIR --remove_dir $TMP_DIR
# make dup info time from start
inter_d=$(date +%s)
diff_d=$(( inter_d - inter_c ))
echo "Dup Info Creation Time elapsed: $diff_d seconds."

# End time
end=$(date +%s)
difference=$(( end - start ))

echo "--- Processing Time Summary ----"
echo "Loading Time elapsed: $diff_a seconds."
echo "Making Suffix Time elapsed: $diff_b seconds."
echo "Collect Time elapsed: $diff_c seconds."
echo "Dup Info Creation Time elapsed: $diff_d seconds."
echo "Total Time elapsed: $difference seconds."
