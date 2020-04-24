file = open('2014_state_total_population.csv')

lines = []

line = file.readline().replace('\n', '')
while line != "":
    lines.append(line.split(','))
    line = file.readline().replace('\n', '')


# drop headings
lines = lines[1:]

table_name = "2014_state_total_population"

sql = ""

for line in lines:
    sql += "INSERT INTO " + table_name + " VALUES (" + line[0] + ",'" + line[1] + "'," + line[2] + ");\n"

outputFile = open('insert_statements.sql', 'a')
outputFile.write(sql)
outputFile.flush()
outputFile.close()
