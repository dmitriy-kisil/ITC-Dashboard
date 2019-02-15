import pandas as pd

df = pd.read_csv("itctray3.csv", index_col=0)
df1 = df.groupby("author")["counts", "Date"].sum().reset_index()
df1 = df1.sort_values(by="counts", ascending=False).reset_index(drop=True)
print(df1.head(10))
