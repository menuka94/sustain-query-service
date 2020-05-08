from csv import reader

file = open('acs5_2011_population_counties.csv')

lines = []

for line in reader(file):
    lines.append(line)

# drop headings
lines = lines[1:]

table_name = "2011_county_total_population"

sql = ""

for line in lines:
    sql += "INSERT INTO " + table_name + " VALUES (" + line[0] + ",'" + line[1] + "'," + line[2] + ");\n"

outputFile = open('insert_statements.sql', 'w')
outputFile.write(sql)
outputFile.flush()
outputFile.close()
