# comando para acceder al nano: nano convertir_excel.py
# Enlace de descarga desde la fuente original: https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx" -O Online_Retail.xlsx
# comando para ejecutar nano: python3 convertir_excel.py
import pandas as pd

# Lee el archivo Excel (asegúrate de que está en el mismo directorio)
df = pd.read_excel("Online_Retail.xlsx")

# Guarda el archivo como CSV
df.to_csv("online_retail.csv", index=False)

print(" Archivo convertido correctamente a CSV: online_retail.csv")
