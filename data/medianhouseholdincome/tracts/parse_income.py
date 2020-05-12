from csv import reader

file = open('acs5_2011_medianhouseholdincome_tracts.csv')

lines = []

for line in reader(file):
    lines.append(line)

# drop headings
lines = lines[1:]

table_name = "2011_tract_medianhouseholdincome"

sql = ""

for line in lines:
    if float(line[2]) > 0:
        sql += "INSERT INTO " + table_name + " VALUES (" + line[0] + ",'" + line[1] + "'," + line[2] + ");\n"
    else:
        print("Negative value.")

outputFile = open('insert_statements.sql', 'w')
outputFile.write(sql)
outputFile.flush()
outputFile.close()
