import os
import argparse

import numpy as np
import pandas as pd


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("dpath", type=str)

INTERESTING_ACCS = [4004, 4603, 4654, 5001, 5017, 5201, 5211, 5293, 5575, 5401, 5476]

le_curr = pd.read_excel("Sample/LE _ currencies.xlsx")
le_curr = le_curr.rename(columns={"Legal Entity Number": "LEGAL_ENTITY", "Functional Currency": "TO_CURRENCY_CODE"})

translation_file = pd.read_excel("Sample/translationrates.xlsx")
translation_rates2 = translation_file[["TO_CURRENCY_CODE","PERIOD_NAME", "EOP_RATE"]]
translation_rates_with_usd = translation_rates2.drop_duplicates("PERIOD_NAME").assign(TO_CURRENCY_CODE="USD", EOP_RATE=1.0)
translation_rates2 = translation_rates2.append(translation_rates_with_usd)
le_eop_rates = pd.merge(le_curr, translation_rates2, left_on="TO_CURRENCY_CODE", right_on="TO_CURRENCY_CODE", how = 'left')


def list_files(dir_path):
    if len(list(os.listdir(dir_path))) == 0:
        raise AssertionError("Can not traverse an empty directory")
    for file in list(os.listdir(dir_path)):
        yield file


def get_initial_data_frame(dir_path, f_path) -> pd.DataFrame:
    return pd.read_csv(f"{dir_path}/{f_path}")


def main():
    args = parser.parse_args()
    print(f"The files are in - {args.dpath}")
    for f in list_files(args.dpath):
        init_df = get_initial_data_frame(args.dpath, f)
        init_df.fillna(0, inplace=True)
        print(f"The present df from {f} file {init_df.shape} shape")
        
        segments = pd.Series(init_df["CONCATENATED_SEGMENTS"]).str.split(".")
        le = segments.str[0]
        acc_num = segments.str[2]
        init_df["LEGAL_ENTITY"] = le.astype(float)
        init_df["ACCOUNT_NUMBER"] = acc_num.astype(int)
        processed_df = init_df[init_df["ACCOUNT_NUMBER"].isin(INTERESTING_ACCS)]
        print(f"The processed df from {f} file {processed_df.shape} shape")


if __name__ == "__main__":
    main()
