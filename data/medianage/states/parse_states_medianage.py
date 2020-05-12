file = open('acs5_2011_medianage_states.csv')

lines = []

line = file.readline().replace('\n', '')
while line != "":
    lines.append(line.split(','))
    line = file.readline().replace('\n', '')


# drop headings
lines = lines[1:]

table_name = "2011_state_medianage"

sql = ""

for line in lines:
    sql += "INSERT INTO " + table_name + " VALUES (" + line[0] + "," + line[2] + ");\n"

outputFile = open('insert_statements.sql', 'w')
outputFile.write(sql)
outputFile.flush()
outputFile.close()
