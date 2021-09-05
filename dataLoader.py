import csv
import psycopg2

def csv_reader(file_obj):
    """
    Read a csv file
    """
    _list = []
    i = 0
    reader = csv.reader(file_obj, skipinitialspace=True)
    for row in reader:

        if row[0] == '#':
            print('ROWS-NAMES')
        else:
            print(f'ROWS-DATA№{i}')
        _list.append(row)
        i+=1
        # for item in range(len(row)-1):
        #     print((row[item]))
    # print(_list)
    # print(len(_list))
    # print(_list[233])



if __name__ == "__main__":
    csv_path = "data.csv"
    with open(csv_path, "r") as f_obj:
        csv_reader(f_obj)

    con = psycopg2.connect(
        database="testl",
        user="postgres",
        password="1",
        host="127.0.0.1",
        port="5432"
    )

    if con:
        print('connected')

    cur = con.cursor()

    cur.execute('''drop TABLE if exists logbox.STUDENT ;''')

    cur.execute('''CREATE TABLE if not exists logbox.STUDENT
         (ADMISSION INT PRIMARY KEY NOT NULL,
         NAME TEXT NOT NULL,
         AGE INT NOT NULL,
         COURSE CHAR(50),
         DEPARTMENT CHAR(50));''')

    cur.execute(
        '''
        INSERT INTO logbox.STUDENT (
        ADMISSION,NAME,AGE,COURSE,DEPARTMENT) 
        VALUES (3420, 'John', 18, 'Computer Science', 'ICT')
        '''
    )

    print("Table created successfully")
    con.commit()
    con.close()