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
reader = ''
count = 0


# https://pythonru.com/biblioteki/operacii-insert-update-delete-v-postgresql

def row_recording(string, connection, cursor):
    #  Recording new row in DATABASE_TABLE
    global clear_database_records_count
    id = string[0]
    question = string[1]
    rightAnswer = string[3]
    commentForJudge = string[4]
    aboutQuestion = string[5]
    link = string[6]
    wrongAnswerOne = string[7]
    wrongAnswerTwo = string[8]
    wrongAnswerThree = string[9]
    themeOfQuestion = string[10]
    questionCategory = string[11]
    sectionOfQuestion = string[12]
    complexityOfQuestion = string[14]
    user = string[16]
    approve = 1
    date_ques_sub = string[17]
    base_date = string[20]
    sql_query = 'INSERT INTO "dataLoader_questions" (id, question,"rightAnswer","commentForJudge","aboutQuestion",' \
                'link,"wrongAnswerOne","wrongAnswerTwo","wrongAnswerThree","themeOfQuestion","questionCategory"' \
                ',"sectionOfQuestion","complexityOfQuestion","user",approve,date_ques_sub,base_date) ' \
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    data = (
        id, question, rightAnswer, commentForJudge, aboutQuestion, link, wrongAnswerOne,
        wrongAnswerTwo,
        wrongAnswerThree, themeOfQuestion, questionCategory, sectionOfQuestion,
        complexityOfQuestion, user, approve,
        date_ques_sub, base_date)
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
        sqlTableDataStructure = cursor.fetchall()
        character_maximum_length = []
        for item in sqlTableDataStructure:
            character_maximum_length.append(item)
        character_maximum_length = dict(character_maximum_length)
        file_list_data, file_dict_data = read_csv_file()

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
    # except Exception:
    #     print('Something gone wrong')
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed.')


def read_csv_file():
    #  Reading CSV file and return list of lists and list of dicts
    csv_path = CSV_PATH
    try:
        with open(csv_path, "r", encoding='utf-8', newline='') as f_obj:
            reader = csv.reader(f_obj, skipinitialspace=True)
            CSV_FILE_DATA = list(reader)
        if CSV_FILE_DATA[0][0] != '#':
            return False,False
        else:
            CSV_FILE_DATA.pop(0)
            CSV_FILE_DATA_LIST_OF_DICTS = []
            for item in CSV_FILE_DATA:
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
                CSV_FILE_DATA_LIST_OF_DICTS.append(elm_dict)
            return CSV_FILE_DATA, CSV_FILE_DATA_LIST_OF_DICTS
    except FileNotFoundError:
        print(f'csv-file [{CSV_PATH}] not found :(')


def main():
    global connection
    global clear_database_records_count
    global new_records_count
    global reader
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

        reader, reader_dicts = read_csv_file()
        if not reader:
            print(f'Error in parsing file [{CSV_PATH}]:\n first cell value must be [#]\n '
                  f'first string must be without any data\n run [check_csv_file] command ')
        else:
            postgreSelectAll = 'select * from "dataLoader_questions" order by "id"'
            cursor.execute(postgreSelectAll)
            sqlData = cursor.fetchall()
            if len(sqlData) == 0:
                #  Database is empty, doing filing
                print('Database is empty. Loading DATA into it')
                sleep(3)
                for string in reader:
                    if string[0] != '#':
                        row_recording(string, connection, cursor)
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
                for item in range(len(reader)):
                    try:
                        if sqlData[item][0] == int(reader[item][0]):
                            #  Checking available rows for changes
                            updated = False
                            updated_fields = []
                            for index in range(18):
                                #  Checking data in columns and UPDATE if mismatch
                                if sqlData[item][index] != reader_dicts[item][fields[index]]:
                                    sql_update_query = f'Update "dataLoader_questions" set "{fields[index]}" = %s where {fields[0]} = %s'
                                    cursor.execute(sql_update_query,
                                                   (reader_dicts[item][fields[index]], reader_dicts[item][fields[0]]))
                                    connection.commit()
                                    updated = True
                                    updated_fields.append(fields[index])
                            if updated:
                                print(
                                    f'Record with ID "{reader_dicts[item][fields[0]]}" updated in "{updated_fields}" field{"s" if len(updated_fields) > 1 else ""}.')
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
        print('Database or connection error\n', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed.')


if __name__ == "__main__":
    check_csv_file()
    # main()
    # clear_db()
