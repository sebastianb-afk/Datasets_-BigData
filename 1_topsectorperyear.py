#Subir y arreglar datos  =======================================================================================
#Arranca el spark
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
#Me lee los datos
nasdaq_data = sc.textFile("/home/ubuntu/Datasets_-BigData/NASDAQsample.csv").map(lambda line: line.split(','))
company_list = sc.textFile('/home/ubuntu/Datasets_-BigData/companylist.tsv').map(lambda line: line.split('\t'))
#Junta las cosas que necesito de nasda, el año y el volumen y de company el sector, y uno según simbolo de ambos
nasdaq = nasdaq_data.map(lambda x: (x[1],(x[2][:4],int(x[7]))))
company = company_list.map(lambda x: (x[0],x[3]))
sucio = nasdaq.join(company)

#Ajusto los datos por facilidad para mi análisis
final = sucio.map(lambda x: ([x[0], x[1][1], x[1][0][0], x[1][0][1]]))

#===============================================================================================================
#volumen por sector separado
sector_volumen = final.map(lambda x: ((x[2], x[1]), x[3]))  #((año, sector), volumen acción)
#Acumula
sector_total = sector_volumen.reduceByKey(lambda a, b: a + b)
#Cluster por años
anos = sector_total.map(lambda x: (x[0][0], (x[0][1],x[1])))

grupoanos = anos.groupByKey()

fanos = grupoanos.mapValues(lambda y: max(y, key=lambda x: x[1]))

ffanos = fanos.map(lambda x: f'{x[1][0]},{x[0]},{x[1][1]}')
ffanos.saveAsTextFile('1_out')
