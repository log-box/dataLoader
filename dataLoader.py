import csv
import datetime
from time import sleep

import psycopg2
from psycopg2 import Error

USER = 'postgres'
# USER = 'postgress' https://pythonru.com/biblioteki/operacii-insert-update-delete-v-postgresql  сравнивать значения с 2 по 13
PASSWORD = '1'
DATABASE = 'dataloader'
PORT = '5432'
HOST = '127.0.0.1'
CSV_PATH = 'data.csv'


def clear_db():
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
        print('WARNING!!!\nThis operations will DELET ALL DATA IN DATABASE!\n')
        sleep(5)
        userInput = input('Are you sure?[y/n]\n')
        if userInput.lower() == 'y':
            postgreDeleteAll = 'TRUNCATE "dataLoader_questions" RESTART IDENTITY CASCADE;'
            cursor.execute(postgreDeleteAll)
            connection.commit()
            print(f'Database "{DATABASE}" was cleaned !!!')
        else:
            print('Operation was canceled!!! DATA was NOT deleted!')

    except (Exception, Error) as error:
        print('DB connection error', error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed')


def read_csv_file():
    csv_path = CSV_PATH
    try:
        with open(csv_path, "r", encoding='utf-8', newline='') as f_obj:
            reader = csv.reader(f_obj, skipinitialspace=True)
            CSV_FILE_DATA = list(reader)
        if CSV_FILE_DATA[0][0] != '#':
            return False
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
    connection = ''
    i = 1
    reader = ''
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
                  f'first string must be without any data')
        else:
            postgreSelectAll = 'select * from "dataLoader_questions"'
            cursor.execute(postgreSelectAll)
            sqlData = cursor.fetchall()
            print(f'Records in database: {len(sqlData)}')
            if len(sqlData) == 0:
                print('Database is empty. Loading DATA into it')
                sleep(3)
                for string in reader:
                    if string[0] != '#':
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
                        sqlData = 'INSERT INTO "dataLoader_questions" (id, question,"rightAnswer","commentForJudge","aboutQuestion",' \
                                  'link,"wrongAnswerOne","wrongAnswerTwo","wrongAnswerThree","themeOfQuestion","questionCategory"' \
                                  ',"sectionOfQuestion","complexityOfQuestion","user",approve,date_ques_sub,base_date) ' \
                                  'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                        data = (
                            id, question, rightAnswer, commentForJudge, aboutQuestion, link, wrongAnswerOne,
                            wrongAnswerTwo,
                            wrongAnswerThree, themeOfQuestion, questionCategory, sectionOfQuestion,
                            complexityOfQuestion, user, approve,
                            date_ques_sub, base_date)
                        cursor.execute(sqlData, data)
                        print(f'Строка№ {i} записана успешно')
                        i += 1
                        connection.commit()
            else:
                fields = (
                    "id", "user", "question", "linkOfPicture", "rightAnswer", "wrongAnswerOne", "wrongAnswerTwo",
                    "wrongAnswerThree", "commentForJudge", "aboutQuestion", "link"
                    , "themeOfQuestion", "questionCategory", "sectionOfQuestion", "complexityOfQuestion",
                    "approve", "date_ques_sub", "base_date")
                for item in range(len(sqlData) - 1):
                    if sqlData[item][0] == int(reader[item][0]):
                        for i in range(18):
                            if sqlData[item][i] != reader_dicts[item][fields[i]]:
                                sql_update_query = f'Update "dataLoader_questions" set {fields[i]} = %s where {fields[0]} = %s'
                                cursor.execute(sql_update_query, (reader_dicts[item][fields[i]], reader_dicts[item][fields[0]]))
                                connection.commit()
                                count = cursor.rowcount
                                print("Запись успешно обновлена")
                                # print(f'ELM: #{item} data mismatch in {sqlData[item][i]}\n{reader_dicts[item][fields[i]]}')
                print(f'Total records update: {count}')
    except KeyError:
        print('Error in indexes\nCall developer +79200631679')
    except (Exception, Error) as error:
        if i < len(reader):
            print(f'Loading is not completed\nTotal records to load: [{len(reader)}]\nLoaded [{i - 1}] records')
        print('Database or connection error\n', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed')


if __name__ == "__main__":
    main()
    # clear_db()
