# comando para acceder al nano: nano convertir_excel.py
# Enlace de descarga desde la fuente original: https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx" -O Online_Retail.xlsx
# comando para ejecutar: python3 convertir_excel.py
import pandas as pd

# Leer el archivo Excel 
df = pd.read_excel("Online_Retail.xlsx")

# Guardar el CSV
df.to_csv("online_retail.csv", index=False)
# Salida
print(" Archivo convertido correctamente a CSV: online_retail.csv")
