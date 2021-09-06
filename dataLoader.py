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
            cur.execute(
                f'''
                INSERT INTO dataLoader_questions(
                question,
                linkOfPicture,
                rightAnswer,
                commentForJudge,
                aboutQuestion,
                link,
                wrongAnswerOne,
                wrongAnswerTwo,
                wrongAnswerThree,
                themeOfQuestion,
                questionCategory,
                sectionOfQuestion,
                complexityOfQuestion,
                user,
                date_ques_sub,
                base_date
                )
                VALUES (
                {string[1]}, 
                {string[2]}, 
                {string[3]}, 
                {string[4]}, 
                {string[5]},
                {string[6]},
                {string[7]},
                {string[8]},
                {string[9]},
                {string[10]},
                {string[11]},
                {string[12]},
                {string[14]},
                {string[16]},
                {string[17]},
                {string[20]}
                )
                '''
            )
            print(f'Строка№ {i} записана успешно')
            i += 1
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

    con.commit()
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
