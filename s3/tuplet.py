
from pyspark.sql import SparkSession
import xml

INS_COLUMN = {1: 'Response_Code', 2: 'Relationship_Code', 3: 'Maintenance_Code', 4: 'Maintenance_Reason_Code',
              5: 'Benefit_Status', 6: 'Medicare_Status_Code', 8: 'Employment_Status', 11: 'Date_Time_Format',
              12: 'Date_of_Death'}
INS_VALUES = {'18': 'Self', '030': 'Audit or Compare', 'XN': 'Notification Only', '28': 'Initial Enrollment',
              'AI': ' No Reason Given', 'AC': 'Active', 'TE': 'Terminated', 'XN': 'Notification Only',
              '28': 'Initial Enrollment', 'AI': 'No Reason Given', 'A': 'Active', 'C': 'Medicare Part A & B'
              }
REF_1_COLUMN = {2: '1_Reference_ID'}
REF_1_VALUES = {}
REF_2_COLUMN = {2: "2_Reference_ID"}
N3_COLUMN = {1: "ADRESS_1", 2: "ADRESS_2"}

COLUMN_VALUES_MAP = {'INS*Y': [INS_COLUMN, INS_VALUES], 'REF*0F': [REF_1_COLUMN, REF_1_VALUES],
                     'REF*F6': [REF_2_COLUMN, {}], 'N3': [N3_COLUMN, {}]}

#SEGMENT_START = ['INS*Y', 'REF*0F', 'REF*F6', 'N3']


def parseData(dataString):
    records = dataString.split('~INS*')
    bgnData = records[0]
    bgnRecord = records[0].split('~')[3]
    records = records[1:]
    print(bgnRecord.split('*'))
    print(bgnRecord)
    listOfRecords = []
    for record in records:
        dataDic = {}
        record = '~INS*' + record
        segments = record.split('~')
        for index in range(1, len(segments)):
            segment = segments[index]
            columns = None
            elements = segments[index].split('*')
            if(segment.startswith('NM1*31')):
                 columns = COLUMN_VALUES_MAP.get('N3_MAILING',None)
            else:
                columns = COLUMN_VALUES_MAP.get(elements[0], None)
                if (columns is None and len(elements) > 1):
                    columns = COLUMN_VALUES_MAP.get(elements[0] + '*' + elements[1], None)
            if (columns is not None):
                prepareelementdata(segments[index], dataDic, columns[0], columns[1])
        listOfRecords.append(dataDic)
    return listOfRecords


def prepareelementdata(segment, dataDic, columnNames, columnValues):
    data = segment.split('*')
    for index in range(1, len(data)):
        columnName = getColumnName(columnNames, index)
        if (columnName is None):
            continue
        columnValue = getColumnValue(columnValues, data[index])
        dataDic[columnName] = columnValue


def getColumnName(columnMap, key):
    return columnMap.get(key, None)


def getColumnValue(columnValueMap, key):
    return columnValueMap.get(key, key)


spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
# df = spark.read.csv("/tmp/resources/zipcodes.csv")
df1 = spark.read.options(header='true').text('/Users/ashoktentu/PycharmProjects/pythonProject/s3/edi.txt')

print(type(df1))
# df = spark.sparkContext.textFile('/Users/ashoktentu/PycharmProjects/pythonProject/s3/edi.txt')
# df1 =df.map(lambda x: (x, )).toDF()
a='ashok'
a='ashok'
ds = df1.select("value").collect()
print(type(ds))
print(parseData(ds[0]['value']))
df = spark.createDataFrame(parseData(ds[0]['value']))
print(type(df))
df.show()