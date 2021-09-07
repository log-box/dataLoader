import csv

import psycopg2


# conn = psycopg2.connect(connection)
# cursor = conn.cursor()
# items = pickle.load(open(pickle_file,"rb"))
#  https://khashtamov.com/ru/postgresql-python-psycopg2/
# for item in items:
#     city = item[0]
#     price = item[1]
#     info = item[2]
#
#     query =  "INSERT INTO items (info, city, price) VALUES (%s, %s, %s);"
#     data = (info, city, price)
#
#     cursor.execute(query, data)


def csv_reader(file_obj):
    """
    Read a csv file
    """
    _list = []
    i = 1
    reader = csv.reader(file_obj, skipinitialspace=True)
    con = psycopg2.connect(
        database="dataloader",
        user="postgres",
        password="1",
        host="127.0.0.1",
        port="5432"
    )
    if con:
        print('connected')
    for string in reader:
        if string[0] != '#':
            cur = con.cursor()
            question = string[1]
            linkOfPicture = string[2]
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
            sqlData = 'INSERT INTO "dataLoader_questions" (question,"linkOfPicture","rightAnswer","commentForJudge","aboutQuestion",link,"wrongAnswerOne","wrongAnswerTwo","wrongAnswerThree","themeOfQuestion","questionCategory","sectionOfQuestion","complexityOfQuestion","user",approve,date_ques_sub,base_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
            data = (
            question, linkOfPicture, rightAnswer, commentForJudge, aboutQuestion, link, wrongAnswerOne, wrongAnswerTwo,
            wrongAnswerThree, themeOfQuestion, questionCategory, sectionOfQuestion, complexityOfQuestion, user, approve,
            date_ques_sub, base_date)

            cur.execute(sqlData, data)
            print(f'Строка№ {i} записана успешно')
            print(f'{string[1]}')
            print(f'{string[2]}')
            i += 1
            con.commit()
        #     print(f'==============================ЗАПИСЬ[{i}]==============================')
        #     print(f'question: {string[1]}')
        #     print(f'linkOfPicture: {string[2]}')
        #     print(f'rightAnswer: {string[3]}')
        #     print(f'commentForJudge: {string[4]}')
        #     print(f'aboutQuestion: {string[5]}')
        #     print(f'link: {string[6]}')
        #     print(f'wrongAnswerOne: {string[7]}')
        #     print(f'wrongAnswerTwo: {string[8]}')
        #     print(f'wrongAnswerThree: {string[9]}')
        #     print(f'themeOfQuestion: {string[10]}')
        #     print(f'questionCategory: {string[11]}')
        #     print(f'sectionOfQuestion: {string[12]}')
        #     print(f'complexityOfQuestion: {string[14]}')
        #     print(f'user: {string[16]}')
        #     print(f'date_ques_sub: {string[17]}')
        #     print(f'base_date: {string[20]}')
        #     print(f'=======================================================================')
        #     i += 1


    con.close()


if __name__ == "__main__":
    csv_path = "data.csv"
    with open(csv_path, "r", encoding='utf-8', newline='') as f_obj:
        csv_reader(f_obj)

    # cur.execute('''drop TABLE if exists test ;''')
    #
    # cur.execute('''CREATE TABLE if not exists test
    #      (ADMISSION INT PRIMARY KEY NOT NULL,
    #      NAME TEXT NOT NULL,
    #      AGE INT NOT NULL,
    #      COURSE CHAR(50),
    #      DEPARTMENT CHAR(50));''')
    #

    #
    # print("Table created successfully")
    # con.commit()
    # con.close()
