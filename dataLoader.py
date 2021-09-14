import csv
import datetime
from time import sleep

import psycopg2
from psycopg2 import Error

USER = 'postgres'
PASSWORD = '1'
DATABASE = 'dataloader'
DATABASE_TABLE = "dataLoader_questions"
PORT = '5432'
HOST = '127.0.0.1'
CSV_PATH = 'data.csv'


connection = ''
clear_database_records_count = 1
new_records_count = 0
count = 0


# https://pythonru.com/biblioteki/operacii-insert-update-delete-v-postgresql

def row_recording(row, connection, cursor):
    #  Recording new row in DATABASE_TABLE
    sql_query = 'INSERT INTO "dataLoader_questions" ("id", "question","linkOfPicture","rightAnswer",' \
                '"commentForJudge","aboutQuestion","link","wrongAnswerOne","wrongAnswerTwo","wrongAnswerThree",' \
                '"themeOfQuestion","questionCategory","sectionOfQuestion","complexityOfQuestion","user","approve",' \
                '"date_ques_sub","base_date") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); '
    data = tuple(row.values())
    cursor.execute(sql_query, data)
    connection.commit()


def clear_db():
    #  Completely clearing DATABASE_TABLE by CASCADE method
    connection = ''
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()
        print('!!! WARNING !!!\nTHIS OPERATION WILL DELET ALL DATA IN DATABASE!\n')
        sleep(5)
        userInput = input('Are you sure?[y/n]\n')
        if userInput.lower() == 'y':
            postgreDeleteAll = 'TRUNCATE "dataLoader_questions" RESTART IDENTITY CASCADE;'
            cursor.execute(postgreDeleteAll)
            connection.commit()
            print(f'!!! Database "{DATABASE}" was cleaned !!!')
        else:
            print('Operation was canceled!!! DATA was NOT deleted!')

    except (Exception, Error) as error:
        print('DB connection error', error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed')


def check_csv_file():
    global connection
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()
        sql_structure_request = "select column_name,  character_maximum_length from INFORMATION_SCHEMA.COLUMNS where table_name ='dataLoader_questions';"
        cursor.execute(sql_structure_request)
        sql_table_data_structure = cursor.fetchall()
        character_maximum_length = dict([item for item in sql_table_data_structure])
        file_dict_data = read_csv_file()
        if not file_dict_data:
            return print('CSV file error in first row. Check first cell value, must be [#]')
        errors = []
        for dicts in file_dict_data:
            for key, value in dicts.items():
                if type(value) is str:
                    value_lenght = len(value)
                    if character_maximum_length[key] < value_lenght:
                        errors.append(
                            f'error in row [{dicts["id"]}] field [{key}] with [{len(value)}] witch more than [{character_maximum_length[key]}] char length in database field')
        if errors:
            with open('errors.txt', "w", encoding='utf-8') as log_file:
                for line in errors:
                    log_file.write(line + '\n')
                print('Errors in [errors.txt]')
        else:
            print('Cheking CSV file is complete. File is correct')
    except (Exception, Error) as error:
        print(f'Database or connection error\n{error}')
    finally:
        if connection:
            cursor.close()
            connection.close()


def read_csv_file():
    #  Reading CSV file and return list of lists and list of dicts
    csv_path = CSV_PATH
    try:
        with open(csv_path, "r", encoding='utf-8', newline='') as f_obj:
            reader = csv.reader(f_obj, skipinitialspace=True)
            csv_file_data = list(reader)
        if csv_file_data[0][0] != '#':
            return False
        else:
            csv_file_data.pop(0)
            csv_file_data_list_of_dicts = []
            for item in csv_file_data:
                date_ques_sub = datetime.datetime.strptime(item[17], '%d.%m.%Y').strftime('%Y-%m-%d')
                base_date = datetime.datetime.strptime(item[20], '%d.%m.%Y').strftime('%Y-%m-%d')
                elm_dict = dict(
                    id=int(item[0]),
                    question=item[1],
                    linkOfPicture=None,
                    rightAnswer=item[3],
                    commentForJudge=item[4],
                    aboutQuestion=item[5],
                    link=item[6],
                    wrongAnswerOne=item[7],
                    wrongAnswerTwo=item[8],
                    wrongAnswerThree=item[9],
                    themeOfQuestion=item[10],
                    questionCategory=item[11],
                    sectionOfQuestion=item[12],
                    complexityOfQuestion=int(item[14]),
                    user=item[16],
                    approve=1,
                    date_ques_sub=datetime.datetime.strptime(date_ques_sub, '%Y-%m-%d').date(),
                    base_date=datetime.datetime.strptime(base_date, '%Y-%m-%d').date(),
                )
                csv_file_data_list_of_dicts.append(elm_dict)
            return csv_file_data_list_of_dicts
    except FileNotFoundError:
        print(f'csv-file [{CSV_PATH}] not found :(')


def main():
    global connection
    global clear_database_records_count
    global new_records_count
    global count
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()

        csv_file_data_dicts = read_csv_file()
        if not csv_file_data_dicts:
            print(f'Error in parsing file [{CSV_PATH}]: run [check_csv_file] command ')
        else:
            postgreSelectAll = 'select * from "dataLoader_questions" order by "id"'
            cursor.execute(postgreSelectAll)
            sqlData = cursor.fetchall()
            if len(sqlData) == 0:
                #  Database is empty, doing filing
                print('Database is empty. Loading DATA into it')
                sleep(3)
                for row in csv_file_data_dicts:
                    row_recording(row, connection, cursor)
                    clear_database_records_count += 1
                print(
                    f'Total row{"s" if clear_database_records_count > 1 else ""} {clear_database_records_count} successfully added')
            else:
                #  Data base not empty
                print(f'Records in database: {len(sqlData)}')
                fields = (
                    "id", "user", "question", "linkOfPicture", "rightAnswer", "wrongAnswerOne", "wrongAnswerTwo",
                    "wrongAnswerThree", "commentForJudge", "aboutQuestion", "link"
                    , "themeOfQuestion", "questionCategory", "sectionOfQuestion", "complexityOfQuestion",
                    "approve", "date_ques_sub", "base_date")
                for item in range(len(sqlData)):
                    # temp = tuple(reader_dicts[item].values())
                    if sqlData[item][0] == csv_file_data_dicts[item]['id']:

                    try:
                        if sqlData[item][0] == int(reader[item][0]):
                            #  Checking available rows for changes
                            updated = False
                            updated_fields = []
                            for index in range(18):
                                #  Checking data in columns and UPDATE if mismatch
                                if sqlData[item][index] != csv_file_data_dicts[item][fields[index]]:
                                    sql_update_query = f'Update "dataLoader_questions" set "{fields[index]}" = %s where {fields[0]} = %s'
                                    cursor.execute(sql_update_query,
                                                   (csv_file_data_dicts[item][fields[index]], csv_file_data_dicts[item][fields[0]]))
                                    connection.commit()
                                    updated = True
                                    updated_fields.append(fields[index])
                            if updated:
                                print(
                                    f'Record with ID "{csv_file_data_dicts[item][fields[0]]}" updated in "{updated_fields}" field{"s" if len(updated_fields) > 1 else ""}.')
                                count += 1
                    except IndexError:
                        #  If rows in CSV file more than in DB. Adding new rows
                        row_recording(reader[item], connection, cursor)
                        new_records_count += 1
                        print(f'New data with ID "{reader[item][0]}" successfully added')
                if count > 0:
                    print(f'Total records updated: {count}')
                else:
                    print('There no any changes in current questions. Data not updated.')
                if new_records_count > 0:
                    print(f'Added {new_records_count} new record{"s" if new_records_count > 1 else ""}')
    except KeyError:
        print('Error in indexes\nCall developer +79200631679')
    except (Exception, Error) as error:
        if clear_database_records_count < len(reader):
            print(
                f'Loading is not completed\nTotal records to load: [{len(reader)}]\nLoaded [{clear_database_records_count - 1}] records')
        print('Database or connection error\n',error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed.')


if __name__ == "__main__":
    # check_csv_file()
    main()
    # clear_db()
