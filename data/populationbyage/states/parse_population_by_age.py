file = open('2011_state_population_by_age.csv')

lines = []

line = file.readline().replace('\n', '')
while line != "":
    lines.append(line.split(','))
    line = file.readline().replace('\n', '')


# drop headings
lines = lines[1:]

table_name = "2011_state_population_by_age"

sql = ""

for line in lines:
    sql += "INSERT INTO " + table_name + " VALUES (" + line[0] + ",'" + line[1] + "'," \
        + line[3] + ","+ line[4]  \
            + ");\n"

outputFile = open('insert_statements.sql', 'a')
outputFile.write(sql)
outputFile.flush()
outputFile.close()
