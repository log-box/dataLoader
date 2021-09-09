import csv

import psycopg2
from psycopg2 import Error

USER = 'postgres'
# USER = 'postgress'
PASSWORD = '1'
DATABASE = 'dataloader'
PORT = '5432'
HOST = '127.0.0.1'
CSV_PATH = 'data.csv'


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


def data_loader(csv_object):
    pass

def csv_reader(file_obj):
    # connection = False
    # cursor = False
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor()
        reader = csv.reader(file_obj, skipinitialspace=True)
        if next(reader)[0] != '#':
            print(f'Error in parsing file [{file_obj.name}]:\n first item.value must be [#]\n '
                  f'first string must be without DB-data')
        else:
            try:
                postgreSelectAll = 'select * from "dataLoader_questions"'
                cursor.execute(postgreSelectAll)
                sqlData = cursor.fetchall()
                print(len(sqlData))
                if len(sqlData) == 0:
                    #  Database is empty. Loading DATA into it
                    i = 1
                    for string in reader:
                        if string[0] != '#':
                            # cur = con.cursor()
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
                            sqlData = 'INSERT INTO "dataLoader_questions" (question,"rightAnswer","commentForJudge","aboutQuestion",' \
                                      'link,"wrongAnswerOne","wrongAnswerTwo","wrongAnswerThree","themeOfQuestion","questionCategory"' \
                                      ',"sectionOfQuestion","complexityOfQuestion","user",approve,date_ques_sub,base_date) ' \
                                      'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                            data = (
                                question, rightAnswer, commentForJudge, aboutQuestion, link, wrongAnswerOne,
                                wrongAnswerTwo,
                                wrongAnswerThree, themeOfQuestion, questionCategory, sectionOfQuestion,
                                complexityOfQuestion, user, approve,
                                date_ques_sub, base_date)
                            cursor.execute(sqlData, data)
                            print(f'Строка№ {i} записана успешно')
                            i += 1
                            connection.commit()
                else:
                    for item in range(len(sqlData)-1):
                        print(sqlData[item][0])
                # for row in sqlData:
                #     print(row[1])
                # for string in reader:

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
    csv_path = CSV_PATH
    with open(csv_path, "r", encoding='utf-8', newline='') as f_obj:
        csv_reader(f_obj)
    # clear_db()


