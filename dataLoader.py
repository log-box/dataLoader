import csv

import psycopg2
from psycopg2 import Error

USER = 'postgres'
# USER = 'postgress' https://pythonru.com/biblioteki/operacii-insert-update-delete-v-postgresql  сравнивать значения с 2 по 13
PASSWORD = '1'
DATABASE = 'dataloader'
PORT = '5432'
HOST = '127.0.0.1'
CSV_PATH = 'data.csv'
CSV_FILE_DATA = ''


def clear_db():
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()
        userInput = input('WARNING!!!\nThis operations will DELET ALL DATA IN DATABASE!\nAre you sure?[y/n]\n')
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
            return CSV_FILE_DATA
    except FileNotFoundError:
        print(f'csv-file [{CSV_PATH}] not found :(')


def main():
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()
        reader = read_csv_file()
        if not reader:
            print(f'Error in parsing file [{CSV_PATH}]:\n first cell value must be [#]\n '
                  f'first string must be without any data')
        else:
            try:
                postgreSelectAll = 'select * from "dataLoader_questions"'
                cursor.execute(postgreSelectAll)
                sqlData = cursor.fetchall()
                print(len(sqlData))
                if len(sqlData) == 0:
                    print('Database is empty. Loading DATA into it')
                    i = 1
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
                    for item in range(len(sqlData) - 1):
                        if sqlData[item][0] == int(reader[item][0]):
                            print(f'DB record ID {sqlData[item][0]} and ID {reader[item][0]} from CSV file are match')


            finally:
                pass

    except (Exception, Error) as error:
        print('DB connection error', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection closed')


if __name__ == "__main__":
    main()
    # clear_db()
