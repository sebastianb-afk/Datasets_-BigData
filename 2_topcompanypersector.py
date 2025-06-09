# Subir y arreglar datos =======================================================================================
# Arranca el spark
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
# Leer los datos
nasdaq_data = sc.textFile("/home/ubuntu/Datasets_-BigData/NASDAQsample.csv").map(lambda line: line.split(','))
company_list = sc.textFile('/home/ubuntu/Datasets_-BigData/companylist.tsv').map(lambda line: line.split('\t'))
# Junta los datos que necesito
nasdaq = nasdaq_data.map(lambda x: (x[1], (x[2][:4], float(x[3]), float(x[6]))))# (símbolo, (año, precio apertura, precio cierre))
company = company_list.map(lambda x: (x[0], x[3]))  # (símbolo, sector)
# Une ambos conjuntos de datos según el símbolo
sucio = nasdaq.join(company)  # (símbolo, ((año, precio apertura, precio cierre), sector))
# Ajusto los datos para facilidad de análisis
final = sucio.map(lambda x: ([x[0], x[1][1], x[1][0][0], x[1][0][1], x[1][0][2]]))  # (símbolo, sector, año, apertura, cierre)

#===============================================================================================================
# Calcular crecimiento por sector y año
crecimiento = final.map(lambda x: ((x[2], x[1]), (x[0], round(100*(x[4] - x[3])/x[4], 3) if x[4] != 0 else 0)))  # ((año, sector), (símbolo, crecimiento))

# Agrupar por año y sector
grupo_crecimiento = crecimiento.groupByKey()  # (año, sector), [(símbolo, crecimiento), ...]

# Calcular el máximo crecimiento
max_crecimiento = grupo_crecimiento.mapValues(lambda y: max(y, key=lambda x: x[1]))  # [(símbolo, crecimiento más alto)]

resultado = max_crecimiento.map(lambda x: f'{x[0][1]},{x[0][0]},{x[1][0]},{x[1][1]}%')  # sector, año, simbolo, crecimiento
# Guardar los resultados
resultado.saveAsTextFile('2_out')
