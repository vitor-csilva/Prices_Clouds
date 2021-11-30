from Database.conn_database import SQL

sql = SQL("root", "123456", "localhost", "clouds")
result = sql.find_machine(num_cpu="32", memory="256 GiB")

for row in result:
    print(row)