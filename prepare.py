import os
import argparse

import numpy as np
import pandas as pd


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("dpath", type=str)

INTERESTING_ACCS = ["4004", "4603", "4654", "5001", "5017", "5201", "5211", "5293", "5575", "5401", "5476"]

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

    outputfile = "final_data.csv"
    turn = 0

    for f in list_files(args.dpath):
        init_df = get_initial_data_frame(args.dpath, f)
        init_df.fillna(0, inplace=True)
        print(f"The present df from {f} file {init_df.shape} shape")
        
        segments = pd.Series(init_df["CONCATENATED_SEGMENTS"]).str.split(".")
        le = segments.str[0]
        division = segments[1]
        acc_num = segments.str[2]
        dept = segments[3]
        area = segments[4]
        merchandise_dept = segments[5]
        inter_unit = segments[6]

        init_df["LEGAL_ENTITY"] = le.astype(float)
        init_df["DIVISION"] = division.astype(str)
        init_df["ACCOUNT_NUMBER"] = acc_num.astype(str)
        init_df["DEPARTMENT"] = dept.astype(str)
        init_df["AREA"] = area.astype(str)
        init_df["MARCHANDISE_DEPT"] = merchandise_dept.astype(str)
        init_df["INTER_UNIT"] = inter_unit.astype(str)

        processed_df = init_df[init_df["ACCOUNT_NUMBER"].isin(INTERESTING_ACCS)]
        print(f"The processed df from {f} file {processed_df.shape} shape")

        result_data = pd.merge(processed_df, le_eop_rates, left_on = ["LEGAL_ENTITY", "PERIOD_NAME"], right_on=["LEGAL_ENTITY", "PERIOD_NAME"], how = 'left')
        result_data["ACCOUNTED_DR_USD"] = (result_data["ACCOUNTED_DR"] / result_data["EOP_RATE"]).round(2)
        result_data["ACCOUNTED_CR_USD"] = (result_data["ACCOUNTED_CR"] / result_data["EOP_RATE"]).round(2)
        result_data["ACCOUNTED_BAL_USD"] = result_data["ACCOUNTED_CR_USD"] - result_data["ACCOUNTED_DR_USD"]

        result_data["LAST_UPDATE_DATE"] = pd.to_datetime(result_data["LAST_UPDATE_DATE"]).dt.date
        result_data.dropna(inplace=True)

        result_data.drop(['PERIOD_NAME', 'CONCATENATED_SEGMENTS', 'EOP_RATE', 'ACCOUNTED_DR', 'ACCOUNTED_CR', 'LEGAL_ENTITY', 'TO_CURRENCY_CODE', 'ACCOUNTED_DR_USD', 'ACCOUNTED_CR_USD'], axis=1, inplace=True)

        if "BAL.ACCOUNTED_DR-BAL.ACCOUNTED_CR" in result_data.columns:
            result_data.drop(["BAL.ACCOUNTED_DR-BAL.ACCOUNTED_CR"], axis=1, inplace=True)

        print(f"Result Data dimension {result_data.shape}")

        if turn == 0:
            result_data.to_csv(outputfile, index=False)
            turn += 1
        else:
            result_data.to_csv(outputfile, mode='a', header=False, index=False)
        print(f"Wrote output to {outputfile}")

if __name__ == "__main__":
    main()
