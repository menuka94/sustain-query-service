file = open('geoids.csv')

lines = []

line = file.readline().replace('\n', '')

while line != "":
    lines.append(line.split(','))
    line = file.readline().replace('\n', '')


# drop headings
lines = lines[1:]

table_name = "geoids"

sql = ""
for line in lines:
    sql += "INSERT INTO " + table_name + " (latitude, longitude, state_fips, county_fips, tract_fips) VALUES ("+ line[0] +", "+ line[1] +", "+ line[2] + ", "+ line[3] +", "+ line[4] +");\n" 

outputFile = open('insert_geoids.sql', 'w')
outputFile.write(sql)
outputFile.flush()
outputFile.close()