file = open('filtered_2011_age_states.csv')

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
        + line[3] + ","+ line[4] +"," + line[5] + "," + line[6] + "," + line[7] + "," \
        + line[8] + ","+ line[9] +"," + line[10] + "," + line[11] + "," + line[12] + "," \
        + line[13] + ","+ line[14] +"," + line[15] + "," + line[16] + "," + line[17] + "," \
        + line[18] + ","+ line[19] +"," + line[20] + "," + line[21] + "," + line[22] + "," \
        + line[23] + ","+ line[24] +"," + line[25] + "," + line[26] + "," + line[27] + "," \
        + line[28] + ","+ line[29] +"," + line[30] + "," + line[31] + "," + line[32] + "," \
        + line[33] + ","+ line[34] +"," + line[35] + "," + line[36] + "," + line[37] + "," \
        + line[38] + ","+ line[39] +"," + line[40] + "," + line[42] + "," + line[43] + "," \
        + line[43] + ","+ line[44] +"," + line[45] + "," + line[47] + "," + line[48] + "," \
        + line[49]  \
            + ");\n"

outputFile = open('insert_statements.sql', 'w')
outputFile.write(sql)
outputFile.flush()
outputFile.close()
